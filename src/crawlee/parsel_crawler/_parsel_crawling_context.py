from dataclasses import fields

from parsel import Selector
from typing_extensions import Self

from crawlee.static_content_crawler._static_crawling_context import ParsedHttpCrawlingContext


class ParselCrawlingContext(ParsedHttpCrawlingContext[Selector]):
    @property
    def selector(self) -> Selector:
        return self.parsed_content

    @classmethod
    def from_static_crawling_context(cls, context: ParsedHttpCrawlingContext[Selector]) -> Self:
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(**context_kwargs)
