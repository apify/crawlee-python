from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction

if TYPE_CHECKING:
    from playwright.async_api import Page, Response


@dataclass(frozen=True)
class PlaywrightCrawlingContext(BasicCrawlingContext):
    """Crawling context used by PlaywrightSoupCrawler.

    Args:
        page: The Playwright `Page` object.
        infinite_scroll: Scroll to the bottom of the page, handling loading of additional items.
        response: The Playwright `Response` object.
        enqueue_links: The `PlaywrightCrawler` implementation of the `EnqueueLinksFunction` function.
    """

    page: Page
    infinite_scroll: Callable[[], Awaitable[None]]
    response: Response
    enqueue_links: EnqueueLinksFunction
