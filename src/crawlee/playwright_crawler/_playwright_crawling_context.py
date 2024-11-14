from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from crawlee._utils.docs import docs_group
from crawlee.playwright_crawler._playwright_pre_navigation_context import PlaywrightPreNavigationContext

if TYPE_CHECKING:
    from playwright.async_api import Response

    from crawlee._types import EnqueueLinksFunction


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightCrawlingContext(PlaywrightPreNavigationContext):
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
