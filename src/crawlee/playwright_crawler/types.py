from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee.basic_crawler.types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from playwright.async_api import Page, Response


@dataclass(frozen=True)
class PlaywrightCrawlingContext(BasicCrawlingContext):
    """Crawling context used by PlaywrightSoupCrawler.

    Args:
        page: The Playwright `Page` object.
        response: The Playwright `Response` object.
        enqueue_links: The `PlaywrightCrawler` implementation of the `EnqueueLinksFunction` function.
    """

    page: Page
    response: Response
    enqueue_links: EnqueueLinksFunction
