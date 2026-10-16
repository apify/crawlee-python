from dataclasses import dataclass, fields

from selectolax.lexbor import LexborHTMLParser
from typing_extensions import Self

from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext


# Custom context for Selectolax parser, you can add your own methods here
# to facilitate working with the parsed document.
@dataclass(frozen=True)
class SelectolaxLexborContext(ParsedHttpCrawlingContext[LexborHTMLParser]):
    """Crawling context providing access to the parsed page.

    This context is passed to request handlers and includes all standard
    context methods (push_data, enqueue_links, etc.) plus custom helpers.
    """

    @property
    def parser(self) -> LexborHTMLParser:
        """Convenient alias for accessing the parsed document."""
        return self.parsed_content

    @classmethod
    def from_parsed_http_crawling_context(
        cls, context: ParsedHttpCrawlingContext[LexborHTMLParser]
    ) -> Self:
        """Create custom context from the base context.

        Copies all fields from the base context to preserve framework
        functionality while adding custom interface.
        """
        return cls(
            **{field.name: getattr(context, field.name) for field in fields(context)}
        )
