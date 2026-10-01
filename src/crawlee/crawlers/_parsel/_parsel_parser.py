from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from parsel import Selector
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee._utils.html import decode_html_body, get_declared_html_encoding
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
        # Parsel rejects an empty body, but reads empty text as an empty HTML document.
        if not body:
            return Selector(text='')

        encoding = get_declared_html_encoding(body, content_type)
        text = None if encoding is None else decode_html_body(body, encoding)

        # Parsel detects a JSON object or array as JSON, and servers can send JSON as `text/html`.
        may_be_json = body.lstrip()[:1] in (b'{', b'[') if text is None else text.lstrip()[:1] in ('{', '[')

        # Parsel reads a page starting with an XML declaration as XML, where CSS selectors miss XHTML elements.
        media_type = (content_type or '').split(';')[0].strip().lower()
        selector_type = 'html' if media_type in {'text/html', 'application/xhtml+xml'} and not may_be_json else None

        if text is None:
            return Selector(body=body, type=selector_type)
        return Selector(text=text, type=selector_type)
