# Inspiration:
#
# TODO:
#  - [ ] Cleaning - browser? page?
#  - [ ] Underlying implementation - controllers, plugins
#  - [ ] What about browser contexts?
#  - [x] Parameters for new_page and launch_browser
#  - [x] Operation timeout
#  - [ ] Max open pages per browser
#  - [ ] Retire browser after page count
#  - [ ] Automatically closing inactive browser after
#  - [ ] Automatically retire inactive browser after
#  - [ ] Integrate event manager (why?)

from __future__ import annotations

import asyncio
import itertools
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.browsers.types import BrowserOptions, CrawleePage, PageOptions

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    from crawlee.browsers.base_browser_controller import BaseBrowserController
    from crawlee.browsers.base_browser_plugin import BaseBrowserPlugin

logger = getLogger(__name__)


class BrowserPool:
    """Manages a pool of browsers and their pages, handling lifecycle events and resource allocation.

    This class is responsible for opening and closing browsers, managing pages within those browsers,
    and handling the overall lifecycle of these resources. It provides flexible configuration via
    constructor options, which include various hooks that allow for the insertion of custom behavior
    at different stages of the browser and page lifecycles.
    """

    _GENERATED_PAGE_ID_LENGTH = 8
    """The length of the newly generated page ID."""

    def __init__(
        self,
        plugins: Sequence[BaseBrowserPlugin],
        *,
        max_open_pages_per_browser: int = 20,
        retire_browser_after_page_count: int = 100,
        operation_timeout: timedelta = timedelta(seconds=15),
        close_inactive_browser_after: timedelta = timedelta(minutes=5),
        retire_inactive_browser_after: timedelta = timedelta(seconds=1),
    ) -> None:
        """Create a new instance.

        Args:
            plugins: Browser plugins serve as wrappers around various browser automation libraries,
                providing a consistent interface across different libraries.

            max_open_pages_per_browser: Sets the maximum number of pages that can be open in a single browser
                instance simultaneously. If this limit is reached, a new browser instance will be launched
                to handle any additional pages.

            retire_browser_after_page_count: Browsers can become bloated after processing many pages. This option
                sets the maximum number of pages a browser can process before it automatically retires and closes.
                A new browser will launch to replace it. The browser may retire sooner if the associated `Session`
                is retired. You can adjust session retirement behavior configuring `SessionPool`.

            operation_timeout: Operations of the underlying automation libraries, such as launching a browser
                or opening a new page, can sometimes get stuck. To prevent `BrowserPool` from becoming unresponsive,
                we add a timeout to these operations.

            close_inactive_browser_after: Browsers normally close immediately after their last page is processed.
                However, there could be situations where this does not happen. `BrowserPool` ensures that all inactive
                browsers are regularly closed to free up resources.

            retire_inactive_browser_after: Browsers are marked as retired after being inactive for a certain period.
                This option sets the interval at which browsers are checked and retired if they remain inactive.
                Retired browsers are closed once all their pages are closed.
        """
        self._plugins = plugins
        self._max_open_pages_per_browser = max_open_pages_per_browser
        self._retire_browser_after_page_count = retire_browser_after_page_count
        self._operation_timeout = operation_timeout
        self._close_inactive_browser_after = close_inactive_browser_after
        self._retire_inactive_browser_after = retire_inactive_browser_after

        self._pages = {}  # Track the pages in the pool
        self._plugins_cycle = itertools.cycle(plugins)  # Cycle through the plugins

    @property
    def plugins(self) -> Sequence[BaseBrowserPlugin]:
        """Return the browser plugins."""
        return self._plugins

    @property
    def pages(self) -> Mapping[str, CrawleePage]:
        """Return the pages in the pool."""
        return self._pages

    async def __aenter__(self) -> BrowserPool:
        """Enter the context manager and initialize all browser plugins."""
        logger.info('Initializing browser pool.')
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
        """Exit the context manager and close all browser plugins."""
        logger.info('Closing browser pool.')
        for plugin in self._plugins:
            await plugin.__aexit__(exc_type, exc_value, exc_traceback)

    async def get_new_page(
        self,
        *,
        page_id: str | None = None,
        browser_plugin: BaseBrowserPlugin | None = None,
        page_options: PageOptions | None = None,
    ) -> CrawleePage:
        """Opens a new page in a browser using the specified or a random browser plugin.

        Args:
            page_id: The ID to assign to the new page. If not provided, a random ID is generated.
            browser_plugin: browser_plugin: The browser plugin to use for creating the new page.
                If not provided, the next plugin in the rotation is used.
            page_options: Options to configure the new page.

        Returns:
            The newly created browser page.
        """
        if page_id in self.pages:
            raise ValueError(f'Page with ID: {page_id} already exists.')

        if browser_plugin and browser_plugin not in self.plugins:
            raise ValueError('Provided browser_plugin is not one of the plugins used by BrowserPool.')

        page_id = page_id or crypto_random_object_id(self._GENERATED_PAGE_ID_LENGTH)
        plugin = browser_plugin or next(self._plugins_cycle)
        page_options = page_options or PageOptions()

        return await self._initialize_page(page_id, plugin, page_options)

    async def get_new_page_in_new_browser(
        self,
        *,
        page_id: str | None = None,
        browser_plugin: BaseBrowserPlugin | None = None,
        page_options: PageOptions | None = None,
        browser_options: BrowserOptions | None = None,
    ) -> CrawleePage:
        # will probably be skipped
        pass

    async def get_new_page_with_each_plugin(
        self,
        *,
        page_options: PageOptions | None = None,
    ) -> Sequence[CrawleePage]:
        """Create a new page with each browser plugin in the pool.

        This method is useful for running scripts in multiple environments simultaneously, typically for testing
        or website analysis. Each page is created using a different browser plugin, allowing you to interact
        with various browser types concurrently.

        Args:
            page_options: Options to configure the new pages. These options will be applied to all pages created.
                If not provided, an empty dictionary is used.

        Returns:
            A list of newly created pages, one for each plugin in the pool.
        """
        pages_coroutines = [
            self.get_new_page(browser_plugin=plugin, page_options=page_options) for plugin in self._plugins
        ]
        return await asyncio.gather(*pages_coroutines)

    async def get_browser_controller_by_page(self) -> None:
        pass

    async def retire_browser_controller(self) -> None:
        pass

    async def retire_browser_by_page(self) -> None:
        pass

    async def retire_all_browsers(self) -> None:
        pass

    async def close_all_browsers(self) -> None:
        pass

    async def _initialize_page(
        self,
        page_id: str,
        plugin: BaseBrowserPlugin,
        page_options: PageOptions,
    ) -> CrawleePage | None:
        """Internal method to initialize a new page in a browser using the specified plugin."""
        timeout = self._operation_timeout.total_seconds()

        try:
            raw_page = await asyncio.wait_for(plugin.get_new_page(page_options=page_options), timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Creating a new page with plugin {plugin} timed out.')
            return None

        page = CrawleePage(id=page_id, page=raw_page, browser_type=plugin.browser_type)
        self._pages[page_id] = page
        return page

    async def _pick_browser_with_free_capacity(self) -> BaseBrowserController:
        # Potreba:
        # - max_open_pages_per_browser
        # - activate_pages na controlleru
        #
        # Vrati browser controller, ktery ma volnou kapacitu pro otevreni nove stranky
        # pokud neni zadny, vytvori novy browser controller
        pass

    async def _launch_browser(self) -> BaseBrowserController:
        pass

    async def _create_page_for_browser(self, browser: BaseBrowserController) -> CrawleePage:
        pass

    async def _teardown(self) -> None:
        pass

    async def _get_all_browser_controllers(self) -> None:
        pass

    async def _pick_browser_plugin(self) -> None:
        pass

    async def _close_inactive_retired_browsers(self) -> None:
        pass

    async def _override_page_close(self) -> None:
        pass

    async def _execute_hooks(self) -> None:
        pass

    async def _close_retired_browser_with_no_pages(self) -> None:
        pass

    async def _initialize_fingerprinting(self) -> None:
        pass

    async def _add_fingerprint_hooks(self) -> None:
        pass


#
