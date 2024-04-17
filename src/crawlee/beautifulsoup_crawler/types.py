from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.basic_crawler.types import BasicCrawlingContext
from crawlee.http_crawler.types import HttpCrawlingResult

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


@dataclass(frozen=True)
class BeautifulSoupCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """Crawling context used by BeautifulSoupCrawler."""

    soup: BeautifulSoup
