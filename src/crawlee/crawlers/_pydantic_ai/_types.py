from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel
from typing_extensions import TypeVar

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from parsel import Selector
    from pydantic_ai.usage import RunUsage


TSchema = TypeVar('TSchema', bound=BaseModel)


@docs_group('Functions')
class PydanticAiHtmlDistiller(Protocol):
    """Interface for HTML distillers.

    A distiller reduces raw HTML to a compact representation that an LLM can read cheaply. The built-in
    distillers are `PydanticAiCleanHtmlDistiller` and `PydanticAiSkeletonDistiller`.
    """

    def distill(self, html: str) -> str:
        """Convert raw HTML to a compact representation suitable for an LLM."""

    def get_prompt_notes(self) -> str | None:
        """Return a short description of the produced representation, or `None`."""


@docs_group('Other')
@dataclass
class PydanticAiUsageStats:
    """A lightweight accumulator of token usage across extraction calls."""

    requests: int = 0
    input_tokens: int = 0
    output_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        """The sum of input and output tokens."""
        return self.input_tokens + self.output_tokens

    def add(self, usage: RunUsage) -> None:
        """Accumulate the usage reported by a single run."""
        self.requests += usage.requests
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens


@docs_group('Other')
class PydanticAiHtmlExtractor(Protocol):
    """Interface for HTML extractors.

    An extractor turns an HTML page into a validated Pydantic model using an LLM. The input format (cleaned HTML,
    skeleton, Markdown, ...) is decided by the `PydanticAiHtmlDistiller` an implementation composes. The model and base
    instructions are set at construction. Each `extract` call runs one extraction. The built-in extractors are
    `PydanticAiDirectExtractor` and `PydanticAiSelectorExtractor`.
    """

    async def extract(
        self,
        content: str | Selector,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Extract a structured instance of `schema` from `content`.

        Args:
            content: Raw HTML or a parsed Parsel `Selector`. A `Selector` is the fast path. The crawler passes its
                live parsed tree directly and skips a re-parse. Treat it as read-only, since the user handler shares
                it.
            schema: The Pydantic model describing the desired output.
            scope: Optional CSS selector. Extraction is restricted to the first matching subtree. A scope that matches
                nothing raises an error.
            cache_tag: Optional tag for caching implementations. Selectors are bucketed per tag, so one schema can
                serve several page kinds without competing. The crawler usually passes `request.label`.
                Implementations without caching ignore it.
            additional_instructions: Extra instructions for this call only. They are appended to the base
                instructions, not a replacement. Use them for page specifics (e.g. 'the price is the discounted one,
                not the list price').
        """

    @property
    def ai_usage(self) -> PydanticAiUsageStats:
        """Accumulated token usage across extraction calls."""

    def set_ai_usage(self, value: PydanticAiUsageStats) -> None:
        """Replace the usage accumulator with `value`.

        Lets an external owner share one accumulator across a delegation chain. `PydanticAiSelectorExtractor` uses
        this to fold its fallback's usage into one accumulator. Extractors with per-instance counters may make it
        a no-op.

        Args:
            value: The accumulator to adopt.
        """


@docs_group('Functions')
class ExtractFunction(Protocol):
    """The `extract` helper exposed on `PydanticAiCrawlingContext`.

    Binds the configured extractor to the current page, so a handler passes just the schema and the optional
    per-call knobs.
    """

    async def __call__(
        self,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Extract an instance of `schema` from the current page.

        Args:
            schema: The Pydantic model describing the desired output.
            scope: Optional CSS selector restricting extraction to the first matching subtree. Saves tokens and
                prevents matches outside the region of interest.
            cache_tag: Optional tag used by caching extractors to bucket cached selectors per page kind. Defaults to
                `context.request.label`.
            additional_instructions: Extra instructions appended to the base instructions for this call only (e.g.
                'the price is the discounted one, not the list price'). Does not replace the base instructions.
        """
