# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/playwright/playwright-controller.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from browserforge.fingerprints import Fingerprint, FingerprintGenerator
from playwright.async_api import BrowserContext, Page, ProxySettings
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.browsers._base_browser_controller import BaseBrowserController
from crawlee.browsers._types import BrowserType
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._injected_page_function import create_init_script_with_fingerprint

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

        self._fingerprint_generator = FingerprintGenerator(**(fingerprint_generator_options or {}))

        self._browser_context: BrowserContext | None = None
        self._pages = list[Page]()
        self._last_page_opened_at = datetime.now(timezone.utc)

        self._use_fingerprints = use_fingerprints
        self._fingerprint: Fingerprint | None = None

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
            await self._set_fingerprint()
            await self._set_browser_context(fingerprint=self._fingerprint)

        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        page_options = dict(page_options) if page_options else {}
        page = await self._get_browser_context().new_page(**page_options)

        # Handle page close event
        page.on(event='close', f=self._on_page_close)

        # Update internal state
        self._pages.append(page)
        self._last_page_opened_at = datetime.now(timezone.utc)

        # Inject fingerprint
        if self._use_fingerprints:
            await self._inject_fingerprint_to_page(page)

        return page

    async def _set_browser_context(
        self, proxy_info: ProxyInfo | None = None, fingerprint: Fingerprint | None = None
    ) -> None:
        """Set browser context.

        Set headers based on fingerprint if available to ensure consistency between headers and fingerprint.
        Fallback to header generator if no fingerprint is available.
        """
        if fingerprint:
            headers = fingerprint.headers
        elif self._header_generator:
            common_headers = self._header_generator.get_common_headers()
            sec_ch_ua_headers = self._header_generator.get_sec_ch_ua_headers(browser_type=self.browser_type)
            user_agent_header = self._header_generator.get_user_agent_header(browser_type=self.browser_type)
            headers = dict(common_headers | sec_ch_ua_headers | user_agent_header)
        else:
            headers = None
        self._browser_context = await self._create_browser_context(proxy_info, headers)

    def _get_browser_context(self) -> BrowserContext:
        if not self._browser_context:
            raise RuntimeError('Browser context was not set yet.')
        return self._browser_context

    async def _set_fingerprint(self) -> None:
        if self._use_fingerprints and not self._fingerprint:
            while fingerprint := self._fingerprint_generator.generate():
                if self._is_good_fingerprint(fingerprint):
                    break
            self._fingerprint = fingerprint

    @staticmethod
    def _is_good_fingerprint(fingerprint: Fingerprint) -> bool:
        """Check if fingerprint is ok to use.

        By trial and error it was found out that some generated fingerprints are not working well.
        All fingerprints that were not working well had 'Te': 'trailers' in headers.
        """
        return fingerprint.headers.get('Te', '') != 'trailers'

    def _get_fingerprint(self) -> Fingerprint:
        if not self._use_fingerprints:
            raise RuntimeError('Fingerprint was is not allowed. use_fingerprints = False.')
        if not self._fingerprint:
            raise RuntimeError('Fingerprint was not set yet.')
        return self._fingerprint

    async def _inject_fingerprint_to_page(self, page: Page) -> None:
        await page.add_init_script(create_init_script_with_fingerprint(self._get_fingerprint().dumps()))

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

    async def _create_browser_context(
        self, proxy_info: ProxyInfo | None = None, headers: dict[str, str] | None = None
    ) -> BrowserContext:
        """Create a new browser context with the specified proxy settings."""
        if headers:
            extra_http_headers = headers
            user_agent = headers.get('User-Agent')
        else:
            extra_http_headers = None
            user_agent = None

        proxy = (
            ProxySettings(
                server=f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                username=proxy_info.username,
                password=proxy_info.password,
            )
            if proxy_info
            else None
        )

        return await self._browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_http_headers,
            proxy=proxy,
        )
