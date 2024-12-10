from dataclasses import dataclass, fields

from bs4 import BeautifulSoup
from typing_extensions import Self

from crawlee._utils.docs import docs_group
from crawlee.abstract_http_crawler._http_crawling_context import ParsedHttpCrawlingContext


@dataclass(frozen=True)
@docs_group('Data structures')
class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup]):
    """The crawling context used by the `BeautifulSoupCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    @property
    def soup(self) -> BeautifulSoup:
        """Convenience alias."""
        return self.parsed_content

    @classmethod
    def from_parsed_http_crawling_context(cls, context: ParsedHttpCrawlingContext[BeautifulSoup]) -> Self:
        """Convenience constructor that creates new context from existing `ParsedHttpCrawlingContext[BeautifulSoup]`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})
