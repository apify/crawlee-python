from __future__ import annotations

from dataclasses import dataclass

from bs4 import BeautifulSoup

from crawlee._utils.docs import docs_group
from crawlee.parsers.static_content_parser import ParsedHttpCrawlingContext


@dataclass(frozen=True)
@docs_group('Data structures')
class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup]):
    """The crawling context used by the `BeautifulSoupCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    @property
    def soup(self) -> BeautifulSoup:
        """Property for backwards compatibility."""
        return self.parsed_content
