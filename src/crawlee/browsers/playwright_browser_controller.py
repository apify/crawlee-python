# Inspiration: https://github.com/apify/crawlee/blob/v3.10.0/packages/browser-pool/src/playwright/playwright-controller.ts

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, Sequence

from crawlee.browsers.base_browser_controller import BaseBrowserController

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserType, Page


class PlaywrightBrowserController(BaseBrowserController):
    """Controller for managing Playwright browser instances and their pages."""

    def __init__(self, browser: Browser, browser_type: BrowserType) -> None:
        self._browser = browser
        self._browser_type = browser_type
        self._active_pages = 0

    @property
    def active_pages(self) -> int:
        """Number of active pages managed by the controller."""
        return self._active_pages

    async def new_page(self, context_options: dict | None = None) -> Page:
        """Create a new page with the given context options."""
        context_options = context_options or {}
        page: Page = await self._browser.new_page(**context_options)
        page.once(event='close', f=self._on_page_close)
        self._active_pages += 1
        return page

    async def close(self) -> None:
        """Close the browser."""
        await self._browser.close()  # it closes all pages in the browser

    async def get_cookies(self, page: Page) -> Sequence[Mapping]:
        """Retrieve cookies from a page context."""
        context = page.context
        return await context.cookies()

    async def set_cookies(self, page: Page, cookies: Sequence[Mapping]) -> None:
        """Set cookies to a page context."""
        context = page.context
        await context.add_cookies(cookies)

    def _on_page_close(self) -> None:
        """Handle actions after a page is closed."""
        self._active_pages -= 1
