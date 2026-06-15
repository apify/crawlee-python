from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from parsel import Selector
from pydantic_ai.models import infer_model

from crawlee._utils.docs import docs_group

from ._types import AiUsageStats

if TYPE_CHECKING:
    from pydantic_ai.models import Model
    from pydantic_ai.usage import UsageLimits

    from ._types import AiHtmlDistiller, TSchema


@docs_group('Other')
class BaseAiHtmlExtractor(ABC):
    """Base class for the built-in HTML extractors.

    An HTML extractor turns a page into a validated Pydantic model with the help of an LLM. This abstract base
    implements the parts the built-in extractors share: resolving the model, composing the task instructions with
    the distiller's prompt notes, and accumulating token usage.

    The public interface is the `AiHtmlExtractor` protocol. The concrete extractors are `AiDirectExtractor` and
    `AiSelectorExtractor`.
    """

    def __init__(
        self,
        model: str | Model,
        *,
        distiller: AiHtmlDistiller,
        instructions: str,
        usage_limits: UsageLimits | None,
    ) -> None:
        """Initialize a new instance.

        Args:
            model: A provider-prefixed name (e.g. `'openai:gpt-5.4-nano'`) or a pydantic-ai `Model`. Credentials are
                read from the provider's environment variable (e.g. `OPENAI_API_KEY`) or passed explicitly through a
                `Model` instance.
            distiller: The HTML distiller shaping the LLM input.
            instructions: Base task instructions. The distiller's prompt notes are appended automatically.
            usage_limits: Optional pydantic-ai `UsageLimits` applied to every single run.
        """
        self._model = infer_model(model)
        self._distiller = distiller
        self._base_instructions = self._compose_instructions(instructions, distiller)
        self._usage_limits = usage_limits
        self._ai_usage = AiUsageStats()

    @property
    def ai_usage(self) -> AiUsageStats:
        """Accumulated token usage of this extractor's runs."""
        return self._ai_usage

    def set_ai_usage(self, value: AiUsageStats) -> None:
        """Replace the usage accumulator with `value`.

        Lets an external owner share one accumulator across a delegation chain.

        Args:
            value: The accumulator to adopt.
        """
        self._ai_usage = value

    @abstractmethod
    async def extract(
        self,
        content: str | Selector,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Extract a structured instance of `schema` from `content`."""

    @staticmethod
    def _compose_instructions(instructions: str, distiller: AiHtmlDistiller) -> str:
        """Append the distiller's input-format notes to the task instructions.

        Args:
            instructions: The base task instructions.
            distiller: The distiller producing the LLM input.
        """
        notes = distiller.get_prompt_notes()
        return f'{instructions}\n\n{notes}' if notes else instructions

    @staticmethod
    def _resolve_scope(selector: Selector, scope: str) -> Selector:
        """Return the first subtree matching `scope`, or raise.

        Args:
            selector: The Parsel selector to query.
            scope: A CSS selector identifying the region of interest.

        Raises:
            ValueError: When the scope matches nothing on the page.
        """
        scoped = selector.css(scope)
        if not scoped:
            raise ValueError(f'Extraction scope {scope!r} matched nothing on the page.')
        return scoped[0]

    @staticmethod
    def _as_selector(content: str | Selector) -> Selector:
        """Wrap a raw HTML string in a `Selector`, or return the input unchanged."""
        return content if isinstance(content, Selector) else Selector(text=content)
