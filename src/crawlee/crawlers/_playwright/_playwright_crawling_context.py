from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

from ._playwright_post_nav_crawling_context import PlaywrightPostNavCrawlingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from crawlee._types import EnqueueLinksFunction, ExtractLinksFunction


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class PlaywrightCrawlingContext(PlaywrightPostNavCrawlingContext):
    """The crawling context used by the `PlaywrightCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    enqueue_links: EnqueueLinksFunction
    """The Playwright `EnqueueLinksFunction` implementation."""

    extract_links: ExtractLinksFunction
    """The Playwright `ExtractLinksFunction` implementation."""

    infinite_scroll: Callable[[], Awaitable[None]]
    """A function to perform infinite scrolling on the page. This scrolls to the bottom, triggering
    the loading of additional content if present."""
