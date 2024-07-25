from __future__ import annotations

from dataclasses import dataclass

from crawlee.http_clients import HttpCrawlingResult
from crawlee.types import BasicCrawlingContext


@dataclass(frozen=True)
class HttpCrawlingContext(BasicCrawlingContext, HttpCrawlingResult):
    """HTTP crawling context."""
