from __future__ import annotations

from dataclasses import dataclass, fields

from typing_extensions import Self, override

from crawlee._types import PageSnapshot
from crawlee._utils.docs import docs_group
from crawlee.crawlers._abstract_http._http_crawling_context import HttpCrawlingContext


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class FileDownloadCrawlingContext(HttpCrawlingContext):
    """The crawling context used by the `FileDownloadCrawler`."""

    @classmethod
    def from_http_crawling_context(cls, context: HttpCrawlingContext) -> Self:
        """Initialize a new instance from an existing `HttpCrawlingContext`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    @override
    async def get_snapshot(self) -> PageSnapshot:
        # Downloaded files are binary and streamed bodies cannot be re-read, so there is no page to snapshot.
        return PageSnapshot()
