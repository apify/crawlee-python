# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-plugin.ts

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from playwright.async_api import Playwright, async_playwright
from typing_extensions import override

from crawlee import service_locator
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

    It is a plugin designed to manage browser instances using the Playwright automation library. It acts as a factory
    for creating new browser instances and provides a unified interface for interacting with different browser types
    (chromium, firefox, and webkit). This class integrates configuration options for browser launches (headless mode,
    executable paths, sandboxing, ...). It also manages browser contexts and the number of pages open within each
    browser instance, ensuring that resource limits are respected.
    """

    AUTOMATION_LIBRARY = 'playwright'

    def __init__(
        self,
        *,
        browser_type: BrowserType = 'chromium',
        browser_launch_options: dict[str, Any] | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """A default constructor.

        Args:
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
            browser_launch_options: Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
            browser_new_context_options: Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                Playwright documentation: https://playwright.dev/python/docs/api/class-browser#browser-new-context.
            max_open_pages_per_browser: The maximum number of pages that can be opened in a single browser instance.
                Once reached, a new browser instance will be launched to handle the excess.
        """
        config = service_locator.get_configuration()

        # Default browser launch options are based on the configuration.
        default_launch_browser_options = {
            'headless': config.headless,
            'executable_path': config.default_browser_path,
            'chromium_sandbox': not config.disable_browser_sandbox,
        }

        self._browser_type = browser_type
        self._browser_launch_options = default_launch_browser_options | (browser_launch_options or {})
        self._browser_new_context_options = browser_new_context_options or {}
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
    def browser_launch_options(self) -> Mapping[str, Any]:
        """Return the options for the `browser.launch` method.

        Keyword arguments to pass to the browser launch method. These options are provided directly to Playwright's
        `browser_type.launch` method. For more details, refer to the Playwright documentation:
         https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
        """
        return self._browser_launch_options

    @property
    @override
    def browser_new_context_options(self) -> Mapping[str, Any]:
        """Return the options for the `browser.new_context` method.

        Keyword arguments to pass to the browser new context method. These options are provided directly to Playwright's
        `browser.new_context` method. For more details, refer to the Playwright documentation:
        https://playwright.dev/python/docs/api/class-browser#browser-new-context.
        """
        return self._browser_new_context_options

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
            browser = await self._playwright.chromium.launch(**self._browser_launch_options)
        elif self._browser_type == 'firefox':
            browser = await self._playwright.firefox.launch(**self._browser_launch_options)
        elif self._browser_type == 'webkit':
            browser = await self._playwright.webkit.launch(**self._browser_launch_options)
        else:
            raise ValueError(f'Invalid browser type: {self._browser_type}')

        return PlaywrightBrowserController(
            browser,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
        )
