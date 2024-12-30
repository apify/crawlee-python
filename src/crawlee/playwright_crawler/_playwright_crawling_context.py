from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Callable

from crawlee._utils.docs import docs_group
from crawlee.playwright_crawler._playwright_pre_navigation_context import PlaywrightPreNavigationContext

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from playwright.async_api import Response

    from crawlee._types import EnqueueLinksFunction, _ContextlessEnqueueLinksFunction


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightCrawlingContext(PlaywrightPreNavigationContext):
    """The crawling context used by the `PlaywrightCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    response: Response
    """The Playwright `Response` object containing the response details for the current URL."""

    _enqueue_links: _ContextlessEnqueueLinksFunction[PlaywrightCrawlingContext]
    """The Playwright `EnqueueLinksFunction` implementation."""

    infinite_scroll: Callable[[], Awaitable[None]]
    """A function to perform infinite scrolling on the page. This scrolls to the bottom, triggering
    the loading of additional content if present."""

    @property
    def enqueue_links(self) -> EnqueueLinksFunction:
        """Bind _enqueue_links to this context."""
        return partial(self._enqueue_links, context=self)
