# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-controller.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from playwright.async_api import BrowserContext, Page, ProxySettings
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.browsers._base_browser_controller import BaseBrowserController
from crawlee.browsers._types import BrowserType
from crawlee.fingerprint_suite import HeaderGenerator

if TYPE_CHECKING:
    from collections.abc import Mapping

    from playwright.async_api import Browser

    from crawlee.proxy_configuration import ProxyInfo

from logging import getLogger

logger = getLogger(__name__)


@docs_group('Classes')
class PlaywrightBrowserController(BaseBrowserController):
    """Controller for managing Playwright browser instances and their pages.

    It provides methods to control browser instances, manage their pages, and handle context-specific
    configurations. It enforces limits on the number of open pages and tracks their state.
    """

    AUTOMATION_LIBRARY = 'playwright'
    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        browser: Browser,
        *,
        max_open_pages_per_browser: int = 20,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
    ) -> None:
        """A default constructor.

        Args:
            browser: The browser instance to control.
            max_open_pages_per_browser: The maximum number of pages that can be open at the same time.
            header_generator: An optional `HeaderGenerator` instance used to generate and manage HTTP headers for
                requests made by the browser. By default, a predefined header generator is used. Set to `None` to
                disable automatic header modifications.
        """
        self._browser = browser
        self._max_open_pages_per_browser = max_open_pages_per_browser
        self._header_generator = header_generator

        self._browser_context: BrowserContext | None = None
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

    @property
    @override
    def browser_type(self) -> BrowserType:
        return cast(BrowserType, self._browser.browser_type.name)

    @override
    async def new_page(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        """Create a new page with the given context options.

        Args:
            browser_new_context_options: Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                Playwright documentation: https://playwright.dev/python/docs/api/class-browser#browser-new-context.
            proxy_info: The proxy configuration to use for the new page.

        Returns:
            Page: The newly created page.

        Raises:
            ValueError: If the browser has reached the maximum number of open pages.
        """
        if not self._browser_context:
            self._browser_context = await self._create_browser_context(browser_new_context_options, proxy_info)

        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        page = await self._browser_context.new_page()

        # Handle page close event
        page.on(event='close', f=self._on_page_close)

        # Update internal state
        self._pages.append(page)
        self._last_page_opened_at = datetime.now(timezone.utc)

        return page

    @override
    async def close(self, *, force: bool = False) -> None:
        """Close the browser.

        Args:
            force: Whether to force close all open pages before closing the browser.

        Raises:
            ValueError: If there are still open pages when trying to close the browser.
        """
        if self.pages_count > 0 and not force:
            raise ValueError('Cannot close the browser while there are open pages.')

        if self._browser_context:
            await self._browser_context.close()
        await self._browser.close()

    def _on_page_close(self, page: Page) -> None:
        """Handle actions after a page is closed."""
        self._pages.remove(page)

    async def _create_browser_context(
        self, browser_new_context_options: Mapping[str, Any] | None = None, proxy_info: ProxyInfo | None = None
    ) -> BrowserContext:
        """Create a new browser context with the specified proxy settings."""
        if self._header_generator:
            common_headers = self._header_generator.get_common_headers()
            sec_ch_ua_headers = self._header_generator.get_sec_ch_ua_headers(browser_type=self.browser_type)
            user_agent_header = self._header_generator.get_user_agent_header(browser_type=self.browser_type)
            extra_http_headers = dict(common_headers | sec_ch_ua_headers | user_agent_header)
        else:
            extra_http_headers = None

        browser_new_context_options = dict(browser_new_context_options) if browser_new_context_options else {}
        browser_new_context_options['extra_http_headers'] = browser_new_context_options.get(
            'extra_http_headers', extra_http_headers
        )

        if proxy_info:
            if browser_new_context_options.get('proxy'):
                logger.warning("browser_new_context_options['proxy'] overriden by explicit `proxy_info` argument.")

            browser_new_context_options['proxy'] = ProxySettings(
                server=f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                username=proxy_info.username,
                password=proxy_info.password,
            )

        return await self._browser.new_context(**browser_new_context_options)
