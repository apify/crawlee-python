from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from parsel import Selector
from pydantic import BaseModel
from pydantic_ai import capture_run_messages
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelRequest, UserPromptPart
from pydantic_ai.models.test import TestModel

from crawlee.crawlers import AiDirectExtractor, BaseAiHtmlDistiller
from crawlee.crawlers._ai._prompts import _DIRECT_INSTRUCTIONS

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage


DEFAULT_OUTPUT_ARGS = {'name': 'Phone', 'price': '$9'}


class _Product(BaseModel):
    name: str
    price: str | None = None


class _MockDistiller(BaseAiHtmlDistiller):
    """Distiller returning a fixed marker so the prompt content can be asserted."""

    def distill(self, html: str) -> str:
        _html = html
        return 'MOCK-DISTILLED-HTML'

    def get_prompt_notes(self) -> str | None:
        return 'MOCK-NOTES'


def _extract_model_input(messages: list[ModelMessage]) -> tuple[str, str]:
    """Return the (user prompt, instructions) of the prompt request the model received.

    A run produces several requests (the prompt, then a tool-return follow-up). Only the first carries the user
    prompt, so it is the one to inspect.
    """
    request = next(
        message
        for message in messages
        if isinstance(message, ModelRequest) and any(isinstance(part, UserPromptPart) for part in message.parts)
    )
    prompt = next(part.content for part in request.parts if isinstance(part, UserPromptPart))
    return str(prompt), request.instructions or ''


async def test_returns_validated_model() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS))

    result = await extractor.extract('<html></html>', _Product)

    assert isinstance(result, _Product)
    assert result.name == 'Phone'
    assert result.price == '$9'


async def test_counts_token_usage() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS))

    await extractor.extract('<html></html>', _Product)

    assert extractor.ai_usage.requests == 1
    assert extractor.ai_usage.input_tokens > 0
    assert extractor.ai_usage.output_tokens > 0
    assert extractor.ai_usage.total_tokens > 0


async def test_accepts_selector_input() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS))

    html = '<html><body><div>UNIQUE-CONTENT</div></body></html>'
    with capture_run_messages() as messages:
        await extractor.extract(Selector(text=html), _Product)

    prompt, _ = _extract_model_input(messages)

    html_part = prompt.split('Document:')[1].strip()

    assert html_part == html


async def test_scope_subtree() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS))

    with capture_run_messages() as messages:
        await extractor.extract(
            '<div><article><h1>Phone</h1></article><footer>junk</footer></div>',
            _Product,
            scope='article',
        )

    prompt, _ = _extract_model_input(messages)

    html_part = prompt.split('Document:')[1].strip()

    assert html_part == '<article><h1>Phone</h1></article>'


async def test_scope_raises() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS))

    with pytest.raises(ValueError, match='matched nothing'):
        await extractor.extract('<div>x</div>', _Product, scope='.missing')


async def test_input_prompt() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS), distiller=_MockDistiller())

    with capture_run_messages() as messages:
        await extractor.extract('<html></html>', _Product)

    prompt, _ = _extract_model_input(messages)

    assert 'name, price' in prompt
    assert 'MOCK-DISTILLED-HTML' in prompt


async def test_instructions() -> None:
    extractor = AiDirectExtractor(
        TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS),
        distiller=_MockDistiller(),
    )

    with capture_run_messages() as messages:
        await extractor.extract('<html></html>', _Product)

    _, instructions = _extract_model_input(messages)

    assert 'MOCK-NOTES' in instructions
    assert _DIRECT_INSTRUCTIONS in instructions


async def test_additional_instructions() -> None:
    extractor = AiDirectExtractor(TestModel(custom_output_args=DEFAULT_OUTPUT_ARGS), distiller=_MockDistiller())

    with capture_run_messages() as messages:
        await extractor.extract('<h1>Phone</h1>', _Product, additional_instructions='PER-CALL-HINT')

    _, instructions = _extract_model_input(messages)

    # Both the base instructions and the per-call hint reach the model.
    assert 'PER-CALL-HINT' in instructions
    assert _DIRECT_INSTRUCTIONS in instructions


async def test_raise_for_invalid_output() -> None:
    # `name` is required, so output missing it fails validation on every retry until the run errors.
    extractor = AiDirectExtractor(TestModel(custom_output_args={'price': '$9'}), retries=2)

    with pytest.raises(UnexpectedModelBehavior):
        await extractor.extract('<h1>x</h1>', _Product)

    # The extractor's usage stats reflect the 3 failed attempts (1 initial + 2 retries).
    assert extractor.ai_usage.requests == 3
