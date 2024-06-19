from __future__ import annotations

from dataclasses import dataclass

from crawlee.basic_crawler.types import BasicCrawlingContext
from crawlee.http_clients import HttpCrawlingResult


@dataclass(frozen=True)
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """HTTP crawling context."""
