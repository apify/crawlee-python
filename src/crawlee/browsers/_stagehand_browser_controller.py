from __future__ import annotations

from asyncio import Lock
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from playwright.async_api import Browser, BrowserContext, Page, ProxySettings
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.browsers._browser_controller import BrowserController
from crawlee.browsers._types import StagehandPage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from stagehand import AsyncSession

    from crawlee.browsers._types import BrowserType
    from crawlee.proxy_configuration import ProxyInfo

logger = getLogger(__name__)


@docs_group('Browser management')
class StagehandBrowserController(BrowserController):
    """Controller for managing a Stagehand-controlled browser instance.

    Bridges Crawlee's browser management with Stagehand: provides page creation via
    Playwright (connected to Stagehand's browser via CDP) and exposes the Stagehand
    session so the crawling context can access AI methods (act/extract/observe).
    """

    AUTOMATION_LIBRARY = 'stagehand'

    def __init__(
        self,
        browser: Browser,
        session: AsyncSession,
        *,
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """Initialize a new instance.

        Args:
            browser: Playwright browser connected to Stagehand via CDP.
            session: Active Stagehand session used for AI operations.
            max_open_pages_per_browser: Maximum number of pages open at the same time.
        """
        self._browser = browser
        self._session = session
        self._max_open_pages_per_browser = max_open_pages_per_browser

        self._browser_context: BrowserContext | None = None
        self._pages = list[Page]()
        self._total_opened_pages = 0
        self._opening_pages_count = 0
        self._last_page_opened_at = datetime.now(timezone.utc)
        self._context_creation_lock: Lock | None = None

    @property
    @override
    def pages(self) -> list[Page]:
        return self._pages  # type: ignore[return-value]

    @property
    @override
    def total_opened_pages(self) -> int:
        return self._total_opened_pages

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
        return (self.pages_count + self._opening_pages_count) < self._max_open_pages_per_browser

    @property
    @override
    def is_browser_connected(self) -> bool:
        return self._browser.is_connected()

    @property
    @override
    def browser_type(self) -> BrowserType:
        return 'chromium'

    async def _get_context_creation_lock(self) -> Lock:
        if self._context_creation_lock is None:
            self._context_creation_lock = Lock()
        return self._context_creation_lock

    @override
    async def new_page(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> StagehandPage:
        """Create a new page in the Stagehand-managed browser.

        Args:
            browser_new_context_options: Ignored. Context is managed by Stagehand via CDP.
            proxy_info: Proxy configuration applied when creating the shared browser context.
                All pages share one context, so proxy is fixed on the first call.

        Returns:
            The newly created page.

        Raises:
            ValueError: If the browser has reached the maximum number of open pages.
        """
        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        if browser_new_context_options:
            logger.warning(
                'browser_new_context_options are ignored by StagehandBrowserController. '
                'The existing CDP context is reused.'
            )

        self._opening_pages_count += 1

        try:
            async with await self._get_context_creation_lock():
                if self._browser_context is None:
                    if proxy_info:
                        self._browser_context = await self._browser.new_context(
                            proxy=ProxySettings(
                                server=f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                                username=proxy_info.username,
                                password=proxy_info.password,
                            )
                        )
                    elif self._browser.contexts:
                        # Reuse the existing CDP context when no proxy is needed.
                        self._browser_context = self._browser.contexts[0]
                    else:
                        self._browser_context = await self._browser.new_context()
                elif proxy_info:
                    logger.warning(
                        'proxy_info is ignored for subsequent pages — all pages share the same browser context.'
                    )

            raw_page = await self._browser_context.new_page()
            page = StagehandPage(raw_page, self._session)
            raw_page.on('close', lambda _: self._on_page_close(cast('Page', page)))

            self._pages.append(page)
            self._last_page_opened_at = datetime.now(timezone.utc)
            self._total_opened_pages += 1
        finally:
            self._opening_pages_count -= 1

        return page

    @override
    async def close(self, *, force: bool = False) -> None:
        """End the Stagehand session and close the browser connection.

        Args:
            force: Whether to force close all open pages before closing.

        Raises:
            ValueError: If there are still open pages when closing without force.
        """
        if self.pages_count > 0 and not force:
            raise ValueError('Cannot close the browser while there are open pages.')

        try:
            await self._session.end()
        except Exception:
            logger.warning('Failed to end Stagehand session gracefully.', exc_info=True)

        if self._browser.is_connected():
            await self._browser.close()

    def _on_page_close(self, page: Page) -> None:
        """Handle actions after a page is closed."""
        self._pages.remove(page)
