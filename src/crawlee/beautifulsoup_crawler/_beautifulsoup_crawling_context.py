from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.http_crawler import HttpCrawlingResult
from crawlee.types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


@dataclass(frozen=True)
class BeautifulSoupCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """Crawling context used by BeautifulSoupCrawler."""

    soup: BeautifulSoup
    enqueue_links: EnqueueLinksFunction
