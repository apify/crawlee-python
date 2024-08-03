from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from crawlee.basic_crawler.types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from playwright.async_api import Page, Response


@dataclass(frozen=True)
class PlaywrightCrawlingContext(BasicCrawlingContext):
    """Crawling context used by PlaywrightSoupCrawler.

    Args:
        page: The Playwright `Page` object.
        scroll_to_bottom: Scroll to the bottom of the page, handling "infinite scroll".
        response: The Playwright `Response` object.
        enqueue_links: The `PlaywrightCrawler` implementation of the `EnqueueLinksFunction` function.
    """

    page: Page
    scroll_to_bottom: Callable[[], Awaitable[None]]
    response: Response
    enqueue_links: EnqueueLinksFunction
