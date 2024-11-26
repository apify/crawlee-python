import asyncio
from typing import Iterable

from parsel import Selector
from typing_extensions import override

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee.http_clients import HttpResponse
from crawlee.http_crawler import BlockedInfo, StaticContentParser


class ParselParser(StaticContentParser[Selector]):
    """Parser for parsing http response using Parsel."""

    @override
    async def parse(self, response: HttpResponse) -> Selector:
        return await asyncio.to_thread(lambda: Selector(body=response.read()))

    @override
    def is_blocked(self, parsed_content: Selector) -> BlockedInfo:
        reason = ''
        if parsed_content is not None:
            matched_selectors = [
                selector
                for selector in RETRY_CSS_SELECTORS
                if parsed_content.type in ('html', 'xml') and parsed_content.css(selector).get() is not None
            ]

            if matched_selectors:
                reason = (
                    f"Assuming the session is blocked - HTTP response matched the following selectors: "
                    f"{'; '.join(matched_selectors)}"
                )

        return BlockedInfo(reason=reason)

    @override
    def find_links(self, parsed_content: Selector, selector: str) -> Iterable[str]:
        link: Selector
        urls: list[str] = []
        for link in parsed_content.css(selector):
            if (url := link.xpath('@href').get()) is not None:
                urls.append(url.strip())  # noqa: PERF401  #Mypy has problems using is not None for type inference in list comprehension.
        return urls
