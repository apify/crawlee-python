from __future__ import annotations

from dataclasses import dataclass

from crawlee._types import BasicCrawlingContext
from crawlee.http_clients import HttpCrawlingResult


@dataclass(frozen=True)
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """The crawling context used by the `HttpCrawler`."""
