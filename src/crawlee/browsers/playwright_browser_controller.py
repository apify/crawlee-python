# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-controller.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from playwright.async_api import Page
from typing_extensions import override

from crawlee.browsers.base_browser_controller import BaseBrowserController

if TYPE_CHECKING:
    from collections.abc import Mapping

    from playwright.async_api import Browser

    from crawlee.proxy_configuration import ProxyInfo


class PlaywrightBrowserController(BaseBrowserController):
    """Controller for managing Playwright browser instances and their pages.

    This class provides methods to manage pages within a browser instance, ensuring that the number
    of open pages does not exceed the specified limit and tracking the state of the pages.
    """

    AUTOMATION_LIBRARY = 'playwright'

    def __init__(self, browser: Browser, *, max_open_pages_per_browser: int = 20) -> None:
        """Create a new instance.

        Args:
            browser: The browser instance to control.
            max_open_pages_per_browser: The maximum number of pages that can be open at the same time.
        """
        self._browser = browser
        self._max_open_pages_per_browser = max_open_pages_per_browser

        self._pages = list[Page]()
        self._last_page_opened_at = datetime.now(timezone.utc)

    @property
    @override
    def pages(self) -> list[Page]:
        return self._pages

    @property
    @override
    def pages_count(self) -> int:
        return len(self._pages)

    @property
    @override
    def last_page_opened_at(self) -> datetime:
        return self._last_page_opened_at

    @property
    @override
    def idle_time(self) -> timedelta:
        return datetime.now(timezone.utc) - self._last_page_opened_at

    @property
    @override
    def has_free_capacity(self) -> bool:
        return self.pages_count < self._max_open_pages_per_browser

    @property
    @override
    def is_browser_connected(self) -> bool:
        return self._browser.is_connected()

    @override
    async def new_page(
        self,
        page_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        page_options = dict(page_options) if page_options else {}

        # If "proxy_info" is provided and no proxy is already set in "page_options", configure the proxy.
        if proxy_info and 'proxy' not in page_options:
            page_options['proxy'] = {
                'server': f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                'username': proxy_info.username,
                'password': proxy_info.password,
            }

        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        page = await self._browser.new_page(**page_options)

        # Handle page close event
        page.on(event='close', f=self._on_page_close)

        # Update internal state
        self._pages.append(page)
        self._last_page_opened_at = datetime.now(timezone.utc)

        return page

    @override
    async def close(self, *, force: bool = False) -> None:
        if force:
            for page in self._pages:
                await page.close()

        if self.pages_count > 0:
            raise ValueError('Cannot close the browser while there are open pages.')

        await self._browser.close()

    def _on_page_close(self, page: Page) -> None:
        """Handle actions after a page is closed."""
        self._pages.remove(page)
