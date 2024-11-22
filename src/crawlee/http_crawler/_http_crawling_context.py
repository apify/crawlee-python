from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.http_clients import HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from typing_extensions import Self


@dataclass(frozen=True)
@docs_group('Data structures')
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """The crawling context used by the `_HttpCrawler`."""

    @classmethod
    def from_basic_crawling_context(cls, context: BasicCrawlingContext, http_response: HttpResponse) -> Self:
        """Convenience constructor that creates HttpCrawlingContext from existing BasicCrawlingContext."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(http_response=http_response, **context_kwargs)
