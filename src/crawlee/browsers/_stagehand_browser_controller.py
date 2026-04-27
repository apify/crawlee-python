from __future__ import annotations

from asyncio import Lock
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.browsers._browser_controller import BrowserController
from crawlee.browsers._types import StagehandPage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from playwright.async_api import Browser, BrowserContext, Playwright
    from stagehand import AsyncSession, AsyncStagehand

    from crawlee.browsers._types import BrowserType, StagehandOptions
    from crawlee.proxy_configuration import ProxyInfo


logger = getLogger(__name__)


@docs_group('Browser management')
class StagehandBrowserController(BrowserController):
    """Controller for managing a Stagehand-controlled browser instance.

    Bridges Crawlee's browser management with Stagehand: lazily creates a Stagehand
    session on the first page request (injecting proxy at that point), then connects
    Playwright to it via CDP. All pages share a single browser context per controller.
    """

    AUTOMATION_LIBRARY = 'stagehand'

    def __init__(
        self,
        *,
        playwright: Playwright,
        stagehand_client: AsyncStagehand,
        stagehand_options: StagehandOptions,
        base_launch_options: dict[str, Any],
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """Initialize a new instance.

        Args:
            playwright: Active Playwright instance used to connect to the browser via CDP.
            stagehand_client: Active Stagehand client used to start sessions.
            stagehand_options: Stagehand-specific configuration.
            base_launch_options: Browser launch options (without proxy) built by the plugin.
            max_open_pages_per_browser: Maximum number of pages open at the same time.
        """
        self._playwright = playwright
        self._stagehand_client = stagehand_client
        self._stagehand_options = stagehand_options
        self._base_launch_options = base_launch_options
        self._max_open_pages_per_browser = max_open_pages_per_browser

        self._session: AsyncSession | None = None
        self._browser: Browser | None = None
        self._browser_context: BrowserContext | None = None
        self._session_init_lock = Lock()

        self._pages = list[StagehandPage]()
        self._total_opened_pages = 0
        self._opening_pages_count = 0
        self._last_page_opened_at = datetime.now(timezone.utc)

    @property
    @override
    def pages(self) -> list[StagehandPage]:
        return self._pages

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
        # Session not yet started — controller is available for new pages.
        return self._browser is None or self._browser.is_connected()

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
        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        if browser_new_context_options:
            logger.warning(
                'browser_new_context_options are ignored by StagehandBrowserController. '
                'The existing CDP context is reused.'
            )

        self._opening_pages_count += 1
        try:
            await self._ensure_session(proxy_info)

            if self._browser is None or self._session is None or self._browser_context is None:
                raise RuntimeError('Failed to initialize the browser session.')

            raw_page = await self._browser_context.new_page()
            page = StagehandPage(raw_page, self._session)
            raw_page.on('close', lambda _: self._on_page_close(page))

            self._pages.append(page)
            self._last_page_opened_at = datetime.now(timezone.utc)
            self._total_opened_pages += 1
        finally:
            self._opening_pages_count -= 1

        return page

    @override
    async def close(self, *, force: bool = False) -> None:
        if self.pages_count > 0 and not force:
            raise ValueError('Cannot close the browser while there are open pages.')

        if self._session is None:
            return

        try:
            await self._session.end()
        except Exception:
            logger.warning('Failed to end Stagehand session gracefully.', exc_info=True)

        if self._browser is not None and self._browser.is_connected():
            await self._browser.close()

    def _on_page_close(self, page: StagehandPage) -> None:
        self._pages.remove(page)

    async def _ensure_session(self, proxy_info: ProxyInfo | None = None) -> None:
        if self._session is not None:
            return
        async with self._session_init_lock:
            if self._session is not None:
                return

            opts = self._stagehand_options
            start_kwargs: dict[str, Any] = {
                'model_name': opts.model,
                'verbose': opts.verbose,
                'self_heal': opts.self_heal,
            }
            if opts.dom_settle_timeout_ms is not None:
                start_kwargs['dom_settle_timeout_ms'] = opts.dom_settle_timeout_ms
            if opts.system_prompt is not None:
                start_kwargs['system_prompt'] = opts.system_prompt

            if opts.env == 'LOCAL':
                launch_options: dict[str, Any] = dict(self._base_launch_options)
                if proxy_info:
                    launch_options['proxy'] = {
                        'server': f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                        'username': proxy_info.username or '',
                        'password': proxy_info.password or '',
                    }
                start_kwargs['browser'] = {'type': 'local', 'launch_options': launch_options}
            elif proxy_info:
                logger.warning(
                    'Proxy support in BROWSERBASE mode requires configuring proxies via '
                    'browserbase_session_create_params. proxy_info will be ignored.'
                )

            session: AsyncSession = await self._stagehand_client.sessions.start(**start_kwargs)

            cdp_url = session.data.cdp_url
            if not cdp_url:
                raise RuntimeError(
                    f'No cdp_url returned from Stagehand (env={self._stagehand_options.env!r}). '
                    'Cannot connect Playwright to the browser.'
                )

            self._browser = await self._playwright.chromium.connect_over_cdp(cdp_url)
            self._browser_context = (
                self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
            )
            self._session = session
