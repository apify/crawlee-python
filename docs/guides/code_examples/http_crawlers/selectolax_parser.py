from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from selectolax.lexbor import LexborHTMLParser, LexborNode
from typing_extensions import override

from crawlee.crawlers._abstract_http import AbstractHttpParser

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from crawlee.http_clients import HttpResponse


class SelectolaxLexborParser(AbstractHttpParser[LexborHTMLParser, LexborNode]):
    """Parser for parsing HTTP response using Selectolax Lexbor."""

    @override
    async def parse(self, response: HttpResponse) -> LexborHTMLParser:
        """Parse HTTP response body into a document object."""
        response_body = await response.read()
        # Run parsing in a thread to avoid blocking the event loop.
        return await asyncio.to_thread(LexborHTMLParser, response_body)

    @override
    async def parse_text(self, text: str) -> LexborHTMLParser:
        """Parse raw HTML string into a document object."""
        return LexborHTMLParser(text)

    @override
    async def select(
        self, parsed_content: LexborHTMLParser, selector: str
    ) -> Sequence[LexborNode]:
        """Select elements matching a CSS selector."""
        return tuple(item for item in parsed_content.css(selector))

    @override
    def is_matching_selector(
        self, parsed_content: LexborHTMLParser, selector: str
    ) -> bool:
        """Check if any element matches the selector."""
        return parsed_content.css_first(selector) is not None

    @override
    def find_links(
        self, parsed_content: LexborHTMLParser, selector: str
    ) -> Iterable[str]:
        """Extract href attributes from elements matching the selector.

        Used by `enqueue_links` helper to discover URLs.
        """
        link: LexborNode
        urls: list[str] = []
        for link in parsed_content.css(selector):
            url = link.attributes.get('href')
            if url:
                urls.append(url.strip())
        return urls
