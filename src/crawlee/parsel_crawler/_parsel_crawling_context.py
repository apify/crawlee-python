from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction
from crawlee.http_crawler import HttpCrawlingResult

if TYPE_CHECKING:
    from parsel import Selector


@dataclass(frozen=True)
class ParselCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """Crawling context used by ParselCrawler."""

    selector: Selector
    enqueue_links: EnqueueLinksFunction
