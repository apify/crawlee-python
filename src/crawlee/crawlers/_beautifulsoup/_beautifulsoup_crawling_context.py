from dataclasses import dataclass, fields

from bs4 import BeautifulSoup
from typing_extensions import Self

from crawlee._utils.docs import docs_group
from crawlee.crawlers import ParsedHttpCrawlingContext

from ._utils import html_to_text


@dataclass(frozen=True)
@docs_group('Crawling contexts')
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
        """Initialize a new instance from an existing `ParsedHttpCrawlingContext`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    def html_to_text(self) -> str:
        """Convert the parsed HTML content to newline-separated plain text without tags."""
        return html_to_text(self.parsed_content)
