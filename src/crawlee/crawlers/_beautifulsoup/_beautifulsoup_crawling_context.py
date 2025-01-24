from dataclasses import dataclass, fields
from typing import cast

from bs4 import BeautifulSoup, Tag
from typing_extensions import Self

from crawlee._utils.docs import docs_group
from crawlee.crawlers import ParsedHttpCrawlingContext

from ._utils import html_to_text


@dataclass(frozen=True)
@docs_group('Data structures')
class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[Tag]):
    """The crawling context used by the `BeautifulSoupCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    @property
    def soup(self) -> Tag:
        """Convenience alias."""
        return cast(BeautifulSoup, self.parsed_content)

    @classmethod
    def from_parsed_http_crawling_context(cls, context: ParsedHttpCrawlingContext[Tag]) -> Self:
        """Convenience constructor that creates new context from existing `ParsedHttpCrawlingContext[BeautifulSoup]`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    def html_to_text(self) -> str:
        """Convert the parsed HTML content to newline-separated plain text without tags."""
        return html_to_text(self.parsed_content)
