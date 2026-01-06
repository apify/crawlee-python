from __future__ import annotations

from typing import TYPE_CHECKING

from selectolax.lexbor import LexborHTMLParser, LexborNode

from crawlee.crawlers import AbstractHttpCrawler, HttpCrawlerOptions

from .selectolax_context import SelectolaxLexborContext
from .selectolax_parser import SelectolaxLexborParser

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from typing_extensions import Unpack

    from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext


# Custom crawler using custom context, It is optional and you can use
# AbstractHttpCrawler directly with SelectolaxLexborParser if you don't need
# any custom context methods.
class SelectolaxLexborCrawler(
    AbstractHttpCrawler[SelectolaxLexborContext, LexborHTMLParser, LexborNode]
):
    """Custom crawler using Selectolax Lexbor for HTML parsing."""

    def __init__(
        self,
        **kwargs: Unpack[HttpCrawlerOptions[SelectolaxLexborContext]],
    ) -> None:
        # Final step converts the base context to custom context type.
        async def final_step(
            context: ParsedHttpCrawlingContext[LexborHTMLParser],
        ) -> AsyncGenerator[SelectolaxLexborContext, None]:
            # Yield custom context wrapping with additional functionality around the base
            # context.
            yield SelectolaxLexborContext.from_parsed_http_crawling_context(context)

        # Build context pipeline: HTTP request -> parsing -> custom context.
        kwargs['_context_pipeline'] = (
            self._create_static_content_crawler_pipeline().compose(final_step)
        )
        super().__init__(
            parser=SelectolaxLexborParser(),
            **kwargs,
        )
