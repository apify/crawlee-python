from __future__ import annotations

import sys
from unittest.mock import AsyncMock, patch

import pytest
from pydantic_ai.exceptions import UserError
from pydantic_ai.models.test import TestModel

from crawlee.crawlers import (
    BasicCrawler,
    PydanticAiCrawler,
    PydanticAiDirectExtractor,
    PydanticAiSelectorExtractor,
)


def test_rejects_model_and_extractor_together() -> None:
    with pytest.raises(ValueError, match='at most one'):
        PydanticAiCrawler(model=TestModel(), extractor=PydanticAiDirectExtractor(TestModel()))


def test_default_extractor_is_direct() -> None:
    assert isinstance(PydanticAiCrawler(model=TestModel()).extractor, PydanticAiDirectExtractor)


def test_default_model_when_none_given(monkeypatch: pytest.MonkeyPatch) -> None:
    # The default model is an OpenAI one, whose client needs a key at construction.
    monkeypatch.setenv('OPENAI_API_KEY', 'test-key')

    # With neither model nor extractor, the crawler builds the default direct extractor.
    assert isinstance(PydanticAiCrawler().extractor, PydanticAiDirectExtractor)


def test_emits_experimental_warning() -> None:
    with pytest.warns(UserWarning, match='experimental'):
        PydanticAiCrawler(model=TestModel())


def test_exposes_extractor_and_usage() -> None:
    extractor = PydanticAiDirectExtractor(TestModel())
    crawler = PydanticAiCrawler(extractor=extractor)

    assert crawler.extractor is extractor
    assert crawler.ai_usage is extractor.ai_usage


@pytest.mark.parametrize(
    'extractor_class',
    [
        pytest.param(PydanticAiDirectExtractor, id='direct'),
        pytest.param(PydanticAiSelectorExtractor, id='selector'),
    ],
)
async def test_run_needs_entered_extractor(
    extractor_class: type[PydanticAiDirectExtractor | PydanticAiSelectorExtractor],
) -> None:
    """An extractor passed to the crawler has to be entered before the crawler runs."""
    crawler = PydanticAiCrawler(extractor=extractor_class(TestModel()))

    with pytest.raises(RuntimeError, match=f'{extractor_class.__name__} passed to the crawler is not active'):
        await crawler.run(['https://a.placeholder.com'])


async def test_owns_default_extractor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The crawler enters and exits the default extractor it builds."""
    extractor_enter = AsyncMock()
    extractor_exit = AsyncMock()
    monkeypatch.setattr(PydanticAiDirectExtractor, '__aenter__', extractor_enter)
    monkeypatch.setattr(PydanticAiDirectExtractor, '__aexit__', extractor_exit)
    crawler = PydanticAiCrawler(model=TestModel())

    await crawler.run()

    extractor_enter.assert_awaited_once()
    extractor_exit.assert_awaited_once()


def test_import_error_handled() -> None:
    # The `ai` extra is optional, so accessing the crawler without `pydantic_ai` installed must raise a clear error.
    blocked = {name: None for name in sys.modules if name == 'pydantic_ai' or name.startswith('pydantic_ai.')}
    with patch.dict('sys.modules', blocked):
        for name in list(sys.modules):
            if name.startswith('crawlee.crawlers._pydantic_ai'):
                sys.modules.pop(name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._pydantic_ai import PydanticAiCrawler  # noqa: F401 PLC0415


def test_unknown_model_takes_no_id() -> None:
    """A crawler with an unknown model doesn't use up a crawler id."""
    first_crawler = BasicCrawler()

    with pytest.raises(UserError, match='Unknown model'):
        PydanticAiCrawler(model='no-such-provider:model')

    second_crawler = BasicCrawler()
    assert int(second_crawler._id) == int(first_crawler._id) + 1
