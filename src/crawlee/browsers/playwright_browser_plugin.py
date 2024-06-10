# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-plugin.ts

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any, Literal

from playwright.async_api import Playwright, async_playwright
from typing_extensions import override

from crawlee.browsers.base_browser_plugin import BaseBrowserPlugin
from crawlee.browsers.playwright_browser_controller import PlaywrightBrowserController

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

logger = getLogger(__name__)


class PlaywrightBrowserPlugin(BaseBrowserPlugin):
    """A plugin for managing Playwright automation library.

    It should work as a factory for creating new browser instances.
    """

    AUTOMATION_LIBRARY = 'playwright'

    def __init__(
        self,
        *,
        browser_type: Literal['chromium', 'firefox', 'webkit'] = 'chromium',
        browser_options: Mapping[str, Any] | None = None,
        page_options: Mapping[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """Create a new instance.

        Args:
            browser_type: The type of the browser to launch.
            browser_options: Options to configure the browser instance.
            page_options: Options to configure a new page instance.
            max_open_pages_per_browser: The maximum number of pages that can be opened in a single browser instance.
                Once reached, a new browser instance will be launched to handle the excess.
        """
        self._browser_type = browser_type
        self._browser_options = browser_options or {}
        self._page_options = page_options or {}
        self._max_open_pages_per_browser = max_open_pages_per_browser

        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None

    @property
    @override
    def browser_type(self) -> Literal['chromium', 'firefox', 'webkit']:
        return self._browser_type

    @property
    @override
    def browser_options(self) -> Mapping[str, Any]:
        return self._browser_options

    @property
    @override
    def page_options(self) -> Mapping[str, Any]:
        return self._page_options

    @property
    @override
    def max_open_pages_per_browser(self) -> int:
        return self._max_open_pages_per_browser

    @override
    async def __aenter__(self) -> PlaywrightBrowserPlugin:
        logger.debug('Initializing Playwright browser plugin.')
        self._playwright = await self._playwright_context_manager.__aenter__()
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        logger.debug('Closing Playwright browser plugin.')
        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        if self._browser_type == 'chromium':
            browser = await self._playwright.chromium.launch(**self._browser_options)
        elif self._browser_type == 'firefox':
            browser = await self._playwright.firefox.launch(**self._browser_options)
        elif self._browser_type == 'webkit':
            browser = await self._playwright.webkit.launch(**self._browser_options)
        else:
            raise ValueError(f'Invalid browser type: {self._browser_type}')

        return PlaywrightBrowserController(
            browser,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
        )
