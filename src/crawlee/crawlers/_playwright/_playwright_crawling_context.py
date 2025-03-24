from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from crawlee._types import PageSnapshot
from crawlee._utils.docs import docs_group

from ._playwright_pre_nav_crawling_context import PlaywrightPreNavCrawlingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from playwright.async_api import Response

    from crawlee._types import EnqueueLinksFunction


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightCrawlingContext(PlaywrightPreNavCrawlingContext):
    """The crawling context used by the `PlaywrightCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    response: Response
    """The Playwright `Response` object containing the response details for the current URL."""

    enqueue_links: EnqueueLinksFunction
    """The Playwright `EnqueueLinksFunction` implementation."""

    infinite_scroll: Callable[[], Awaitable[None]]
    """A function to perform infinite scrolling on the page. This scrolls to the bottom, triggering
    the loading of additional content if present."""

    async def get_snapshot(self) -> PageSnapshot:
        html = None
        screenshot = None

        try:
            html = await self.page.content()
        except Exception:
            self.log.exception(f'Failed to get html snapshot for {self.request.url}.')

        try:
            screenshot = await self.page.screenshot()
        except Exception:
            self.log.exception(f'Failed to get page screenshot for {self.request.url}.')

        return PageSnapshot(html=html, screenshot=screenshot)
