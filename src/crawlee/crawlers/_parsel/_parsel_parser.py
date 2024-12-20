import asyncio
from collections.abc import Iterable

from parsel import Selector
from typing_extensions import override

from crawlee.crawlers._abstract_http import AbstractHttpParser
from crawlee.http_clients import HttpResponse


class ParselParser(AbstractHttpParser[Selector]):
    """Parser for parsing HTTP response using Parsel."""

    @override
    async def parse(self, response: HttpResponse) -> Selector:
        return await asyncio.to_thread(lambda: Selector(body=response.read()))

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
