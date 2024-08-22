from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.http_crawler import HttpCrawlingResult
from crawlee.types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from parsel import Selector


@dataclass(frozen=True)
class ParselCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """Crawling context used by ParselCrawler."""

    selector: Selector
    enqueue_links: EnqueueLinksFunction
