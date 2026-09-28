from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from parsel import Selector
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee._utils.html import declared_html_encoding, decode_html_body
from crawlee.crawlers._abstract_http import AbstractHttpParser

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from crawlee.http_clients import HttpResponse


@docs_group('HTTP parsers')
class ParselParser(AbstractHttpParser[Selector, Selector]):
    """Parser for parsing HTTP response using Parsel."""

    @override
    async def parse(self, response: HttpResponse) -> Selector:
        response_body = await response.read()
        content_type = response.headers.get('content-type')
        return await asyncio.to_thread(self._parse_body, response_body, content_type)

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
    def find_links(self, parsed_content: Selector, selector: str, attribute: str) -> Iterable[str]:
        link: Selector
        urls: list[str] = []
        for link in parsed_content.css(selector):
            url = link.xpath(f'@{attribute}').get()
            if url:
                urls.append(url.strip())
        return urls

    @staticmethod
    def _parse_body(body: bytes, content_type: str | None) -> Selector:
        encoding = declared_html_encoding(body, content_type)
        if encoding is None:
            return Selector(body=body)
        return Selector(text=decode_html_body(body, encoding))
