# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-plugin.ts

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from playwright.async_api import Playwright, async_playwright
from typing_extensions import override

from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee.browsers._base_browser_plugin import BaseBrowserPlugin
from crawlee.browsers._playwright_browser_controller import PlaywrightBrowserController

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from crawlee.browsers._types import BrowserType

logger = getLogger(__name__)


@docs_group('Classes')
class PlaywrightBrowserPlugin(BaseBrowserPlugin):
    """A plugin for managing Playwright automation library.

    It should work as a factory for creating new browser instances.
    """

    AUTOMATION_LIBRARY = 'playwright'

    def __init__(
        self,
        *,
        browser_type: BrowserType = 'chromium',
        browser_options: Mapping[str, Any] | None = None,
        page_options: Mapping[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """A default constructor.

        Args:
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
            browser_options: Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
            page_options: Keyword arguments to pass to the new page method. These options are provided directly to
                Playwright's `browser_context.new_page` method. For more details, refer to the Playwright documentation:
                https://playwright.dev/python/docs/api/class-browsercontext#browser-context-new-page.
            max_open_pages_per_browser: The maximum number of pages that can be opened in a single browser instance.
                Once reached, a new browser instance will be launched to handle the excess.
        """
        self._browser_type = browser_type
        self._browser_options = browser_options or {}
        self._page_options = page_options or {}
        self._max_open_pages_per_browser = max_open_pages_per_browser

        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None

        # Flag to indicate the context state.
        self._active = False

    @property
    @override
    def active(self) -> bool:
        return self._active

    @property
    @override
    def browser_type(self) -> BrowserType:
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
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        self._playwright = await self._playwright_context_manager.__aenter__()
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)
        self._active = False

    @override
    @ensure_context
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
