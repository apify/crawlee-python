# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-plugin.ts

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from playwright.async_api import Playwright, async_playwright
from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee.browsers._browser_plugin import BrowserPlugin
from crawlee.browsers._playwright_browser import PlaywrightPersistentBrowser
from crawlee.browsers._playwright_browser_controller import PlaywrightBrowserController

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path
    from types import TracebackType

    from playwright.async_api._generated import Browser

    from crawlee.browsers._types import BrowserType
    from crawlee.fingerprint_suite import FingerprintGenerator

logger = getLogger(__name__)


@docs_group('Classes')
class PlaywrightBrowserPlugin(BrowserPlugin):
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
        user_data_dir: str | Path | None = None,
        browser_launch_options: dict[str, Any] | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
        use_incognito_pages: bool = False,
        fingerprint_generator: FingerprintGenerator | None = None,
    ) -> None:
        """A default constructor.

        Args:
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
            user_data_dir: Path to a User Data Directory, which stores browser session data like cookies and local
                storage.
            browser_launch_options: Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
            browser_new_context_options: Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                Playwright documentation: https://playwright.dev/python/docs/api/class-browser#browser-new-context.
            max_open_pages_per_browser: The maximum number of pages that can be opened in a single browser instance.
                Once reached, a new browser instance will be launched to handle the excess.
            use_incognito_pages: By default pages share the same browser context. If set to True each page uses its
                own context that is destroyed once the page is closed or crashes.
            fingerprint_generator: An optional instance of implementation of `FingerprintGenerator` that is used
                to generate browser fingerprints together with consistent headers.
        """
        config = service_locator.get_configuration()

        # Default browser launch options are based on the configuration.
        default_launch_browser_options: dict[str, Any] = {
            'headless': config.headless,
            'executable_path': config.default_browser_path,
            'chromium_sandbox': not config.disable_browser_sandbox,
        }

        self._browser_type: BrowserType = browser_type
        self._browser_launch_options: dict[str, Any] = default_launch_browser_options | (browser_launch_options or {})
        self._browser_new_context_options = browser_new_context_options or {}
        self._max_open_pages_per_browser = max_open_pages_per_browser
        self._use_incognito_pages = use_incognito_pages
        self._user_data_dir = user_data_dir

        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None

        # Flag to indicate the context state.
        self._active = False

        self._fingerprint_generator = fingerprint_generator

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
        self._playwright_context_manager = async_playwright()
        self._active = False

    @override
    @ensure_context
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        if self._browser_type == 'chromium':
            browser_type = self._playwright.chromium
        elif self._browser_type == 'firefox':
            browser_type = self._playwright.firefox
        elif self._browser_type == 'webkit':
            browser_type = self._playwright.webkit
        else:
            raise ValueError(f'Invalid browser type: {self._browser_type}')

        if self._use_incognito_pages:
            browser: Browser | PlaywrightPersistentBrowser = await browser_type.launch(**self._browser_launch_options)
        else:
            browser = PlaywrightPersistentBrowser(browser_type, self._user_data_dir, self._browser_launch_options)

        return PlaywrightBrowserController(
            browser,
            use_incognito_pages=self._use_incognito_pages,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
            fingerprint_generator=self._fingerprint_generator,
        )
