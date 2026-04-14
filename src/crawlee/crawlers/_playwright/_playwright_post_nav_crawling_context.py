from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

from ._playwright_pre_nav_crawling_context import PlaywrightPreNavCrawlingContext

if TYPE_CHECKING:
    from playwright.async_api import Response


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class PlaywrightPostNavCrawlingContext(PlaywrightPreNavCrawlingContext):
    """The post navigation crawling context used by the `PlaywrightCrawler`.

    It provides access to the `Page` and `Response` objects, after the navigation to the URL is performed.
    """

    response: Response
    """The Playwright `Response` object containing the response details for the current URL."""
