from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Self

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.http_clients import HttpCrawlingResult


@dataclass(frozen=True)
@docs_group('Data structures')
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """The crawling context used by the `HttpCrawler`."""

    def fromBasicCrawlingContext(cls, context = BasicCrawlingContext, http_response=HttpCrawlingResult) -> Self:
        return cls(parsed_content=http_response, **asdict(context))
