from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from bs4 import BeautifulSoup, Tag
from parsel import Selector
from typing_extensions import override

if TYPE_CHECKING:
    from crawlee.crawlers import AbstractHttpParser
    from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
    from crawlee.crawlers._parsel._parsel_parser import ParselParser

TParseResult = TypeVar('TParseResult')
TSelectResult = TypeVar('TSelectResult')


class ParserWithSelect(Generic[TParseResult, TSelectResult], ABC):
    def __init__(self, static_parser: AbstractHttpParser[TParseResult]) -> None:
        self.static_parser = static_parser

    @abstractmethod
    async def select(self, parsed_content: TParseResult, selector: str) -> TSelectResult | None: ...

    @abstractmethod
    async def parse_text(self, text: str) -> TParseResult: ...


class ParselParserWithSelect(ParserWithSelect[Selector, Selector]):
    def __init__(self, static_parser: ParselParser) -> None:
        self.static_parser = static_parser

    @override
    async def select(self, parsed_content: Selector, selector: str) -> Selector | None:
        if selector_list := parsed_content.css(selector):
            return selector_list[0]
        return None

    @override
    async def parse_text(self, text: str) -> Selector:
        return Selector(text=text)


class BeautifulSoupParserWithSelect(ParserWithSelect[BeautifulSoup, Tag]):
    def __init__(self, static_parser: BeautifulSoupParser) -> None:
        self.static_parser = static_parser

    @override
    async def select(self, parsed_content: Tag, selector: str) -> Tag | None:
        return parsed_content.select_one(selector)

    @override
    async def parse_text(self, text: str) -> BeautifulSoup:
        return BeautifulSoup(text, features=self.static_parser.parser)  # type:ignore[attr-defined]  # Mypy bug?
