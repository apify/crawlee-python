# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-controller.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from browserforge.injectors.playwright import AsyncNewContext
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


@docs_group('Classes')
class PlaywrightBrowserController(BaseBrowserController):
    """Controller for managing Playwright browser instances and their pages.

    This class provides methods to manage pages within a browser instance, ensuring that the number
    of open pages does not exceed the specified limit and tracking the state of the pages.
    """

    AUTOMATION_LIBRARY = 'playwright'
    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        browser: Browser,
        *,
        max_open_pages_per_browser: int = 20,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
        use_fingerprints: bool = True,
        fingerprint_generator_options: dict[str, Any] | None = None,
    ) -> None:
        """A default constructor.

        Args:
            browser: The browser instance to control.
            max_open_pages_per_browser: The maximum number of pages that can be open at the same time.
            header_generator: An optional `HeaderGenerator` instance used to generate and manage HTTP headers for
                requests made by the browser. By default, a predefined header generator is used. Set to `None` to
                disable automatic header modifications.
            use_fingerprints: Inject generated fingerprints to page.
            fingerprint_generator_options: Override generated fingerprints with these specific values, if possible.
        """
        self._browser = browser
        self._max_open_pages_per_browser = max_open_pages_per_browser
        self._header_generator = header_generator

        self._browser_context: BrowserContext | None = None
        self._pages = list[Page]()
        self._last_page_opened_at = datetime.now(timezone.utc)

        self._use_fingerprints = use_fingerprints
        self._fingerprint_generator_options = fingerprint_generator_options

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
        page_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        if not self._browser_context:
            await self._set_browser_context(
                page_options=page_options,
                fingerprint_options=self._fingerprint_generator_options,
                proxy_info=proxy_info,
            )

        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        page = await self._get_browser_context().new_page()

        # Handle page close event
        page.on(event='close', f=self._on_page_close)

        # Update internal state
        self._pages.append(page)
        self._last_page_opened_at = datetime.now(timezone.utc)

        return page

    async def _set_browser_context(
        self,
        page_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
        fingerprint_options: dict | None = None,
    ) -> None:
        """Set browser context.

        Create context using `browserforge`  if `_use_fingerprints` is True.
        Create context without fingerprints with headers based header generator if available.
        """
        page_options = page_options or {}
        proxy = (
            ProxySettings(
                server=f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                username=proxy_info.username,
                password=proxy_info.password,
            )
            if proxy_info
            else None
        )

        if self._use_fingerprints:
            self._browser_context = await AsyncNewContext(
                browser=self._browser, fingerprint_options=(fingerprint_options or {}), proxy=proxy, **page_options
            )
            return

        if self._header_generator:
            common_headers = self._header_generator.get_common_headers()
            sec_ch_ua_headers = self._header_generator.get_sec_ch_ua_headers(browser_type=self.browser_type)
            user_agent_header = self._header_generator.get_user_agent_header(browser_type=self.browser_type)
            headers = dict(common_headers | sec_ch_ua_headers | user_agent_header)
            extra_http_headers = headers
        else:
            extra_http_headers = None

        page_options = dict(page_options) if page_options else {}
        page_options['extra_http_headers'] = page_options.get('extra_http_headers', extra_http_headers)

        self._browser_context = await self._browser.new_context(proxy=proxy, **page_options)

    def _get_browser_context(self) -> BrowserContext:
        if not self._browser_context:
            raise RuntimeError('Browser context was not set yet.')
        return self._browser_context

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
