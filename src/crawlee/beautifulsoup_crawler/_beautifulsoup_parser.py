from __future__ import annotations

from typing import TYPE_CHECKING

from bs4 import BeautifulSoup, Tag
from typing_extensions import override

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee.http_crawler import BlockedInfo, StaticContentParser

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.http_clients import HttpResponse


class BeautifulSoupContentParser(StaticContentParser[BeautifulSoup]):
    """Parser for parsing http response using BeautifulSoup."""

    def __init__(self, parser: str = 'lxml') -> None:
        self._parser = parser

    @override
    async def parse(self, response: HttpResponse) -> BeautifulSoup:
        return BeautifulSoup(response.read(), parser=self._parser)

    @override
    def is_blocked(self, parsed_content: BeautifulSoup) -> BlockedInfo:
        reason = ''
        if parsed_content is not None:
            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if parsed_content.select_one(selector) is not None
            ]
            if matched_selectors:
                reason = (
                    f"Assuming the session is blocked - HTTP response matched the following selectors:"
                    f" {'; '.join(matched_selectors)}"
                )
        return BlockedInfo(reason=reason)

    @override
    def find_links(self, parsed_content: BeautifulSoup, selector: str) -> Iterable[str]:
        link: Tag
        urls: list[str] = []
        for link in parsed_content.select(selector):
            if (url := link.attrs.get('href')) is not None:
                urls.append(url.strip())  # noqa: PERF401  #Mypy has problems using is not None for type inference in list comprehension.
        return urls
