# Inspiration: https://github.com/apify/crawlee/blob/v3.10.0/packages/browser-pool/src/playwright/playwright-plugin.ts

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Literal

from playwright.async_api import Playwright, async_playwright
from typing_extensions import override

from crawlee.browsers.base_browser_plugin import BaseBrowserPlugin

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from playwright.async_api import Browser, Page

logger = getLogger(__name__)


class PlaywrightBrowserPlugin(BaseBrowserPlugin):
    """A plugin for managing Playwright browser instances."""

    def __init__(
        self,
        *,
        browser_type: Literal['chromium', 'firefox', 'webkit'] = 'chromium',
        browser_options: Mapping | None = None,
        page_options: Mapping | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            browser_type: The type of the browser to launch.
            browser_options: Options to configure the browser instance.
            page_options: Options to configure a new page instance.
        """
        self._browser_type = browser_type
        self._browser_options = browser_options or {}
        self._page_options = page_options or {}

        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None

    @property
    @override
    def browser(self) -> Browser | None:
        return self._browser

    @property
    @override
    def browser_type(self) -> Literal['chromium', 'firefox', 'webkit']:
        return self._browser_type

    @override
    async def __aenter__(self) -> PlaywrightBrowserPlugin:
        logger.debug('Initializing Playwright browser plugin.')
        self._playwright = await self._playwright_context_manager.__aenter__()

        if self._browser_type == 'chromium':
            self._browser = await self._playwright.chromium.launch(**self._browser_options)
        elif self._browser_type == 'firefox':
            self._browser = await self._playwright.firefox.launch(**self._browser_options)
        elif self._browser_type == 'webkit':
            self._browser = await self._playwright.webkit.launch(**self._browser_options)
        else:
            raise ValueError(f'Invalid browser type: {self._browser_type}')

        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        logger.debug('Closing Playwright browser plugin.')
        if self._browser:
            await self._browser.close()
        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)

    @override
    async def new_page(self) -> Page:
        if not self._browser:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return await self._browser.new_page(**self._page_options)
