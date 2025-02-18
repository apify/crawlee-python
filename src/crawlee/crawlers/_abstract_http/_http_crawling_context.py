from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Generic

from typing_extensions import Self, TypeVar

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction
from crawlee._utils.docs import docs_group
from crawlee.http_clients import HttpCrawlingResult, HttpResponse

TParseResult = TypeVar('TParseResult')
TSelectResult = TypeVar('TSelectResult')


@dataclass(frozen=True)
@docs_group('Data structures')
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """The crawling context used by the `AbstractHttpCrawler`."""

    @classmethod
    def from_basic_crawling_context(cls, context: BasicCrawlingContext, http_response: HttpResponse) -> Self:
        """Convenience constructor that creates `HttpCrawlingContext` from existing `BasicCrawlingContext`."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(http_response=http_response, **context_kwargs)


@dataclass(frozen=True)
@docs_group('Data structures')
class ParsedHttpCrawlingContext(Generic[TParseResult], HttpCrawlingContext):
    """The crawling context used by `AbstractHttpCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    parsed_content: TParseResult
    enqueue_links: EnqueueLinksFunction

    @classmethod
    def from_http_crawling_context(
        cls, context: HttpCrawlingContext, parsed_content: TParseResult, enqueue_links: EnqueueLinksFunction
    ) -> Self:
        """Convenience constructor that creates new context from existing HttpCrawlingContext."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(parsed_content=parsed_content, enqueue_links=enqueue_links, **context_kwargs)
