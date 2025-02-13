from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from parsel import Selector
from typing_extensions import override

from crawlee.crawlers._abstract_http import AbstractHttpParser

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from crawlee.http_clients import HttpResponse


class ParselParser(AbstractHttpParser[Selector, Selector]):
    """Parser for parsing HTTP response using Parsel."""

    @override
    async def parse(self, response: HttpResponse) -> Selector:
        return await asyncio.to_thread(lambda: Selector(body=response.read()))

    @override
    async def parse_text(self, text: str) -> Selector:
        return Selector(text=text)

    @override
    async def select(self, parsed_content: Selector, selector: str) -> Sequence[Selector]:
        return tuple(match for match in parsed_content.css(selector))

    @override
    def is_matching_selector(self, parsed_content: Selector, selector: str) -> bool:
        return parsed_content.type in ('html', 'xml') and parsed_content.css(selector).get() is not None

    @override
    def find_links(self, parsed_content: Selector, selector: str) -> Iterable[str]:
        link: Selector
        urls: list[str] = []
        for link in parsed_content.css(selector):
            url = link.xpath('@href').get()
            if url:
                urls.append(url.strip())
        return urls
