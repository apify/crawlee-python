from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.crawlers._parsel._parsel_crawling_context import ParselCrawlingContext

if TYPE_CHECKING:
    from typing_extensions import Self

    from ._types import AiUsageStats, ExtractFunction


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class AiCrawlingContext(ParselCrawlingContext):
    """The crawling context used by the `AiCrawler`.

    It extends `ParselCrawlingContext`, so the full Parsel `selector` (and `enqueue_links`) remain available
    alongside the AI-powered `extract` helper. Handlers can mix cheap manual selectors with AI extraction on the
    same page.
    """

    extract: ExtractFunction
    """Extract a structured Pydantic model from the page using the configured AI extractor."""

    ai_usage: AiUsageStats
    """The cumulative token usage stats of the extractor across calls in this crawl."""

    @classmethod
    def from_parsel_crawling_context(
        cls,
        context: ParselCrawlingContext,
        *,
        extract: ExtractFunction,
        ai_usage: AiUsageStats,
    ) -> Self:
        """Create a new context from an existing `ParselCrawlingContext`."""
        return cls(
            extract=extract,
            ai_usage=ai_usage,
            **{field.name: getattr(context, field.name) for field in fields(context)},
        )
