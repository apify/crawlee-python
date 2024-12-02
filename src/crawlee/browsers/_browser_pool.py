# Inspiration: https://github.com/apify/crawlee/tree/v3.10.1/packages/browser-pool/

from __future__ import annotations

import asyncio
import itertools
from collections import defaultdict
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

from crawlee._utils.context import ensure_context
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee.browsers._base_browser_controller import BaseBrowserController
from crawlee.browsers._playwright_browser_plugin import PlaywrightBrowserPlugin
from crawlee.browsers._types import BrowserType, CrawleePage

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    from crawlee.browsers._base_browser_plugin import BaseBrowserPlugin
    from crawlee.proxy_configuration import ProxyInfo

logger = getLogger(__name__)


@docs_group('Classes')
class BrowserPool:
    """Manages a pool of browsers and their pages, handling lifecycle events and resource allocation.

    This class is responsible for opening and closing browsers, managing pages within those browsers,
    and handling the overall lifecycle of these resources. It provides flexible configuration via
    constructor options, which include various hooks that allow for the insertion of custom behavior
    at different stages of the browser and page lifecycles.

    The browsers in the pool can be in one of three states: active, inactive, or closed.
    """

    _GENERATED_PAGE_ID_LENGTH = 8
    """The length of the newly generated page ID."""

    def __init__(
        self,
        plugins: Sequence[BaseBrowserPlugin] | None = None,
        *,
        operation_timeout: timedelta = timedelta(seconds=15),
        browser_inactive_threshold: timedelta = timedelta(seconds=10),
        identify_inactive_browsers_interval: timedelta = timedelta(seconds=20),
        close_inactive_browsers_interval: timedelta = timedelta(seconds=30),
    ) -> None:
        """A default constructor.

        Args:
            plugins: Browser plugins serve as wrappers around various browser automation libraries,
                providing a consistent interface across different libraries.
            operation_timeout: Operations of the underlying automation libraries, such as launching a browser
                or opening a new page, can sometimes get stuck. To prevent `BrowserPool` from becoming unresponsive,
                we add a timeout to these operations.
            browser_inactive_threshold: The period of inactivity after which a browser is considered as inactive.
            identify_inactive_browsers_interval: The period of inactivity after which a browser is considered
                as retired.
            close_inactive_browsers_interval: The interval at which the pool checks for inactive browsers
                and closes them. The browser is considered as inactive if it has no active pages and has been idle
                for the specified period.
        """
        self._plugins = plugins or [PlaywrightBrowserPlugin()]
        self._operation_timeout = operation_timeout
        self._browser_inactive_threshold = browser_inactive_threshold

        self._active_browsers = list[BaseBrowserController]()
        """A list of browsers currently active and being used to open pages."""

        self._inactive_browsers = list[BaseBrowserController]()
        """A list of browsers currently inactive and not being used to open new pages,
        but may still contain open pages."""

        self._identify_inactive_browsers_task = RecurringTask(
            self._identify_inactive_browsers,
            identify_inactive_browsers_interval,
        )

        self._close_inactive_browsers_task = RecurringTask(
            self._close_inactive_browsers,
            close_inactive_browsers_interval,
        )

        self._total_pages_count = 0
        self._pages = WeakValueDictionary[str, CrawleePage]()  # Track the pages in the pool
        self._plugins_cycle = itertools.cycle(self._plugins)  # Cycle through the plugins

        # Flag to indicate the context state.
        self._active = False

    @classmethod
    def with_default_plugin(
        cls,
        *,
        browser_type: BrowserType | None = None,
        browser_options: Mapping[str, Any] | None = None,
        page_options: Mapping[str, Any] | None = None,
        headless: bool | None = None,
        **kwargs: Any,
    ) -> BrowserPool:
        """Create a new instance with a single `PlaywrightBrowserPlugin` configured with the provided options.

        Args:
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
            browser_options: Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
            page_options: Keyword arguments to pass to the new page method. These options are provided directly to
                Playwright's `browser_context.new_page` method. For more details, refer to the Playwright documentation:
                https://playwright.dev/python/docs/api/class-browsercontext#browser-context-new-page.
            headless: Whether to run the browser in headless mode.
            kwargs: Additional arguments for default constructor.
        """
        plugin_options: dict = defaultdict(dict)
        plugin_options['browser_options'] = browser_options or {}
        plugin_options['page_options'] = page_options or {}

        if headless is not None:
            plugin_options['browser_options']['headless'] = headless

        if browser_type:
            plugin_options['browser_type'] = browser_type

        plugin = PlaywrightBrowserPlugin(**plugin_options)
        return cls(plugins=[plugin], **kwargs)

    @property
    def plugins(self) -> Sequence[BaseBrowserPlugin]:
        """Return the browser plugins."""
        return self._plugins

    @property
    def active_browsers(self) -> Sequence[BaseBrowserController]:
        """Return the active browsers in the pool."""
        return self._active_browsers

    @property
    def inactive_browsers(self) -> Sequence[BaseBrowserController]:
        """Return the inactive browsers in the pool."""
        return self._inactive_browsers

    @property
    def pages(self) -> Mapping[str, CrawleePage]:
        """Return the pages in the pool."""
        return self._pages

    @property
    def total_pages_count(self) -> int:
        """Returns the total number of pages opened since the browser pool was launched."""
        return self._total_pages_count

    @property
    def active(self) -> bool:
        """Indicates whether the context is active."""
        return self._active

    async def __aenter__(self) -> BrowserPool:
        """Enter the context manager and initialize all browser plugins.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        # Start the recurring tasks for identifying and closing inactive browsers
        self._identify_inactive_browsers_task.start()
        self._close_inactive_browsers_task.start()

        timeout = self._operation_timeout.total_seconds()

        try:
            for plugin in self._plugins:
                await asyncio.wait_for(plugin.__aenter__(), timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Initializing of the browser plugin {plugin} timed out, will be skipped.')

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close all browser plugins.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self._identify_inactive_browsers_task.stop()
        await self._close_inactive_browsers_task.stop()

        for browser in self._active_browsers + self._inactive_browsers:
            await browser.close(force=True)

        for plugin in self._plugins:
            await plugin.__aexit__(exc_type, exc_value, exc_traceback)

        self._active = False

    @ensure_context
    async def new_page(
        self,
        *,
        page_id: str | None = None,
        browser_plugin: BaseBrowserPlugin | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> CrawleePage:
        """Opens a new page in a browser using the specified or a random browser plugin.

        Args:
            page_id: The ID to assign to the new page. If not provided, a random ID is generated.
            browser_plugin: browser_plugin: The browser plugin to use for creating the new page.
                If not provided, the next plugin in the rotation is used.
            proxy_info: The proxy configuration to use for the new page.

        Returns:
            The newly created browser page.
        """
        if page_id in self.pages:
            raise ValueError(f'Page with ID: {page_id} already exists.')

        if browser_plugin and browser_plugin not in self.plugins:
            raise ValueError('Provided browser_plugin is not one of the plugins used by BrowserPool.')

        page_id = page_id or crypto_random_object_id(self._GENERATED_PAGE_ID_LENGTH)
        plugin = browser_plugin or next(self._plugins_cycle)

        return await self._get_new_page(page_id, plugin, proxy_info)

    @ensure_context
    async def new_page_with_each_plugin(self) -> Sequence[CrawleePage]:
        """Create a new page with each browser plugin in the pool.

        This method is useful for running scripts in multiple environments simultaneously, typically for testing
        or website analysis. Each page is created using a different browser plugin, allowing you to interact
        with various browser types concurrently.

        Returns:
            A list of newly created pages, one for each plugin in the pool.
        """
        pages_coroutines = [self.new_page(browser_plugin=plugin) for plugin in self._plugins]
        return await asyncio.gather(*pages_coroutines)

    async def _get_new_page(
        self,
        page_id: str,
        plugin: BaseBrowserPlugin,
        proxy_info: ProxyInfo | None,
    ) -> CrawleePage:
        """Internal method to initialize a new page in a browser using the specified plugin."""
        timeout = self._operation_timeout.total_seconds()
        browser = self._pick_browser_with_free_capacity(plugin)

        try:
            if not browser:
                browser = await asyncio.wait_for(self._launch_new_browser(plugin), timeout)
            page = await asyncio.wait_for(
                browser.new_page(page_options=plugin.page_options, proxy_info=proxy_info),
                timeout,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f'Creating a new page with plugin {plugin} timed out.') from exc
        except RuntimeError as exc:
            raise RuntimeError('Browser pool is not initialized.') from exc

        crawlee_page = CrawleePage(id=page_id, page=page, browser_type=plugin.browser_type)
        self._pages[page_id] = crawlee_page
        self._total_pages_count += 1
        return crawlee_page

    def _pick_browser_with_free_capacity(
        self,
        browser_plugin: BaseBrowserPlugin,
    ) -> BaseBrowserController | None:
        """Pick a browser with free capacity that matches the specified plugin."""
        for browser in self._active_browsers:
            if browser.has_free_capacity and browser.AUTOMATION_LIBRARY == browser_plugin.AUTOMATION_LIBRARY:
                return browser

        return None

    async def _launch_new_browser(self, plugin: BaseBrowserPlugin) -> BaseBrowserController:
        """Launch a new browser instance using the specified plugin."""
        browser = await plugin.new_browser()
        self._active_browsers.append(browser)
        return browser

    def _identify_inactive_browsers(self) -> None:
        """Identify inactive browsers and move them to the inactive list if their idle time exceeds the threshold."""
        for browser in self._active_browsers:
            if browser.idle_time >= self._browser_inactive_threshold:
                self._active_browsers.remove(browser)
                self._inactive_browsers.append(browser)

    async def _close_inactive_browsers(self) -> None:
        """Close the browsers that have no active pages and have been idle for a certain period."""
        for browser in self._inactive_browsers:
            if not browser.pages:
                await browser.close()
                self._inactive_browsers.remove(browser)
