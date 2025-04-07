from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Generic

from typing_extensions import Self, TypeVar

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, ExtractLinksFunction, PageSnapshot
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
        """Initialize a new instance from an existing `BasicCrawlingContext`."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(http_response=http_response, **context_kwargs)

    async def get_snapshot(self) -> PageSnapshot:
        """Get snapshot of crawled page."""
        return PageSnapshot(html=self.http_response.read().decode('utf-8'))


@dataclass(frozen=True)
@docs_group('Data structures')
class ParsedHttpCrawlingContext(Generic[TParseResult], HttpCrawlingContext):
    """The crawling context used by `AbstractHttpCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    parsed_content: TParseResult
    enqueue_links: EnqueueLinksFunction
    extract_links: ExtractLinksFunction

    @classmethod
    def from_http_crawling_context(
        cls,
        context: HttpCrawlingContext,
        parsed_content: TParseResult,
        enqueue_links: EnqueueLinksFunction,
        extract_links: ExtractLinksFunction,
    ) -> Self:
        """Initialize a new instance from an existing `HttpCrawlingContext`."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(
            parsed_content=parsed_content, enqueue_links=enqueue_links, extract_links=extract_links, **context_kwargs
        )
