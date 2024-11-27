from dataclasses import fields

from bs4 import BeautifulSoup
from typing_extensions import Self

from crawlee.static_content_crawler._static_crawling_context import ParsedHttpCrawlingContext


class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup]):
    @property
    def soup(self) -> BeautifulSoup:
        """Property for backwards compatibility."""
        return self.parsed_content

    @classmethod
    def from_static_crawling_context(cls, context: ParsedHttpCrawlingContext[BeautifulSoup]) -> Self:
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(**context_kwargs)
