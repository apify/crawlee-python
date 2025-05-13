from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext, PageSnapshot
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from playwright.async_api import Page

    from ._types import BlockRequestsFunction


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightPreNavCrawlingContext(BasicCrawlingContext):
    """The pre navigation crawling context used by the `PlaywrightCrawler`.

    It provides access to the `Page` object, before the navigation to the URL is performed.
    """

    page: Page
    """The Playwright `Page` object for the current page."""

    block_requests: BlockRequestsFunction
    """Blocks network requests matching specified URL patterns."""

    async def get_snapshot(self) -> PageSnapshot:
        """Get snapshot of crawled page."""
        html = None
        screenshot = None

        try:
            html = await self.page.content()
        except Exception:
            self.log.exception(f'Failed to get html snapshot for {self.request.url}.')

        try:
            screenshot = await self.page.screenshot(full_page=True, type='jpeg')
        except Exception:
            self.log.exception(f'Failed to get page screenshot for {self.request.url}.')

        return PageSnapshot(html=html, screenshot=screenshot)
