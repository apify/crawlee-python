from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from bs4 import BeautifulSoup, Tag
from typing_extensions import override

from crawlee.static_content_crawler._static_content_parser import StaticContentParser

if TYPE_CHECKING:
    from collections.abc import Iterable

    from crawlee.http_clients import HttpResponse


class BeautifulSoupContentParser(StaticContentParser[BeautifulSoup]):
    """Parser for parsing http response using BeautifulSoup."""

    def __init__(self, parser: BeautifulSoupParser = 'lxml') -> None:
        self._parser = parser

    @override
    async def parse(self, response: HttpResponse) -> BeautifulSoup:
        return BeautifulSoup(response.read(), features=self._parser)

    @override
    def is_matching_selector(self, parsed_content: BeautifulSoup, selector: str) -> bool:
        return parsed_content.select_one(selector) is not None

    @override
    def find_links(self, parsed_content: BeautifulSoup, selector: str) -> Iterable[str]:
        link: Tag
        urls: list[str] = []
        for link in parsed_content.select(selector):
            url = link.attrs.get('href')
            if url:
                urls.append(url.strip())
        return urls


BeautifulSoupParser = Literal['html.parser', 'lxml', 'xml', 'html5lib']
