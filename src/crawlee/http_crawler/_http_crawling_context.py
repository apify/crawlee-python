from __future__ import annotations

from dataclasses import dataclass

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.http_clients import HttpCrawlingResult


@dataclass(frozen=True)
@docs_group('Data structures')
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """The crawling context used by the `HttpCrawler`."""
