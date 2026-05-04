from __future__ import annotations

from asyncio import Lock
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.browsers._browser_controller import BrowserController
from crawlee.browsers._stagehand_types import StagehandPage
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._header_generator import fingerprint_browser_type_from_playwright_browser_type

if TYPE_CHECKING:
    from collections.abc import Mapping

    from playwright.async_api import Browser, BrowserContext, Playwright
    from stagehand import AsyncSession, AsyncStagehand

    from crawlee.browsers._stagehand_types import StagehandOptions
    from crawlee.browsers._types import BrowserType
    from crawlee.proxy_configuration import ProxyInfo


logger = getLogger(__name__)


@docs_group('Browser management')
class StagehandBrowserController(BrowserController):
    """Controller for managing a Stagehand-controlled browser instance.

    It creates and connects to the browser lazily on the first ``new_page`` call: Stagehand
    starts a session, and Playwright then connects to it via CDP. All pages share a single
    browser context, as Stagehand creates the browser and its context together during session
    initialisation.
    """

    AUTOMATION_LIBRARY = 'stagehand'
    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        *,
        playwright: Playwright,
        stagehand_client: AsyncStagehand,
        stagehand_options: StagehandOptions,
        max_open_pages_per_browser: int = 20,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
    ) -> None:
        """Initialize a new instance.

        Args:
            playwright: Active Playwright instance used to connect to the browser via CDP.
            stagehand_client: Active Stagehand REST client used to start and end sessions.
            stagehand_options: Stagehand-specific configuration (model, env, self-heal, etc.).
            max_open_pages_per_browser: Maximum number of pages that can be open at the same time.
            header_generator: An optional `HeaderGenerator` instance used to generate and manage HTTP headers for
                requests made by the browser. By default, a predefined header generator is used. Set to `None` to
                disable automatic header modifications.
        """
        self._playwright = playwright
        self._stagehand_client = stagehand_client
        self._stagehand_options = stagehand_options
        self._max_open_pages_per_browser = max_open_pages_per_browser
        self._header_generator = header_generator

        self._session: AsyncSession | None = None
        self._browser: Browser | None = None
        self._browser_context: BrowserContext | None = None
        self._context_creation_lock = Lock()

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
        # Session not yet started - controller is available for new pages.
        return self._browser is None or self._browser.is_connected()

    @property
    @override
    def browser_type(self) -> BrowserType:
        return 'chromium'

    @override
    async def new_page(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> StagehandPage:
        """Create a new page in the Stagehand-managed browser.

        On the first call, starts the Stagehand session with the provided options. On subsequent
        calls, ``browser_new_context_options`` and ``proxy_info`` are ignored because the session
        context cannot be reconfigured once it is running.

        Args:
            browser_new_context_options: Options merged on top of the plugin's launch options
                when creating the first session. Ignored if the session already exists.
            proxy_info: Proxy injected into the session on first creation.

        Raises:
            ValueError: If the browser has reached the maximum number of open pages.
            RuntimeError: If the Stagehand session fails to initialise.
        """
        if not self.has_free_capacity:
            raise ValueError('Cannot open more pages in this browser.')

        self._opening_pages_count += 1
        try:
            # Lazily start a Stagehand session on the first page request, passing proxy and context options
            # at that point.
            if not self._browser_context:
                async with self._context_creation_lock:
                    if not self._browser_context:
                        self._browser_context = await self._create_browser_context(
                            browser_new_context_options=browser_new_context_options,
                            proxy_info=proxy_info,
                        )

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

        if self._session is None and self._browser is None:
            return

        try:
            if self._session is not None:
                try:
                    await self._session.end()
                except Exception:
                    logger.warning('Failed to end Stagehand session gracefully.', exc_info=True)

            if self._browser is not None and self._browser.is_connected():
                await self._browser.close()
        finally:
            self._session = None
            self._browser_context = None

    def _on_page_close(self, page: StagehandPage) -> None:
        self._pages.remove(page)

    def _get_extra_http_headers(
        self,
        extra_http_headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        if extra_http_headers is not None:
            return dict(extra_http_headers)

        if self._header_generator:
            generated_headers = self._header_generator.get_specific_headers(
                header_names={
                    'Accept',
                    'Accept-Language',
                    'User-Agent',
                    'sec-ch-ua',
                    'sec-ch-ua-mobile',
                    'sec-ch-ua-platform',
                },
                browser_type=fingerprint_browser_type_from_playwright_browser_type(self.browser_type),
            )
        else:
            generated_headers = {}

        return dict(generated_headers)

    def _build_session_start_params(
        self,
        browser_new_context_options: dict[str, Any],
        proxy_info: ProxyInfo | None = None,
    ) -> dict[str, Any]:
        session_start_params: dict[str, Any] = {
            'model_name': self._stagehand_options.model,
            'verbose': self._stagehand_options.verbose,
            'self_heal': self._stagehand_options.self_heal,
        }
        launch_options = dict(browser_new_context_options)

        if self._stagehand_options.dom_settle_timeout_ms is not None:
            session_start_params['dom_settle_timeout_ms'] = self._stagehand_options.dom_settle_timeout_ms

        if self._stagehand_options.system_prompt is not None:
            session_start_params['system_prompt'] = self._stagehand_options.system_prompt

        if self._stagehand_options.env == 'LOCAL':
            if proxy_info:
                launch_options['proxy'] = {
                    'server': f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                    'username': proxy_info.username or '',
                    'password': proxy_info.password or '',
                }
            session_start_params['browser'] = {'type': 'local', 'launch_options': launch_options}
        else:
            session_start_params['browser'] = {'type': 'browserbase', 'launch_options': launch_options}
            if proxy_info:
                session_start_params['browserbase_session_create_params'] = {
                    'proxies': [
                        {
                            'type': 'external',
                            'server': f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                            'username': proxy_info.username or '',
                            'password': proxy_info.password or '',
                        }
                    ]
                }

        return session_start_params

    async def _create_browser_context(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> BrowserContext:
        browser_new_context_options = dict(browser_new_context_options) if browser_new_context_options else {}

        extra_http_headers = self._get_extra_http_headers(browser_new_context_options.pop('extra_http_headers', None))

        session_start_params = self._build_session_start_params(
            browser_new_context_options=browser_new_context_options,
            proxy_info=proxy_info,
        )

        session: AsyncSession = await self._stagehand_client.sessions.start(**session_start_params)

        cdp_url = session.data.cdp_url
        if not cdp_url:
            raise RuntimeError(
                f'No cdp_url returned from Stagehand (env={self._stagehand_options.env!r}). '
                'Cannot connect Playwright to the browser.'
            )

        self._browser = await self._playwright.chromium.connect_over_cdp(cdp_url)

        context = self._browser.contexts[0]

        await context.set_extra_http_headers(extra_http_headers)

        self._session = session

        return context
