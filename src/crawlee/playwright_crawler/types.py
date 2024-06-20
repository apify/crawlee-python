from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.basic_crawler.types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from playwright.async_api import Page


@dataclass(frozen=True)
class PlaywrightCrawlingContext(BasicCrawlingContext):
    """Crawling context used by PlaywrightSoupCrawler."""

    page: Page
    enqueue_links: EnqueueLinksFunction
