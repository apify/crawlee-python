from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from playwright.async_api import Playwright, async_playwright
from stagehand import AsyncStagehand
from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee.browsers._browser_plugin import BrowserPlugin
from crawlee.browsers._stagehand_browser_controller import StagehandBrowserController
from crawlee.browsers._types import StagehandOptions

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from crawlee.browsers._browser_controller import BrowserController
    from crawlee.browsers._types import BrowserType

logger = getLogger(__name__)


@docs_group('Browser management')
class StagehandBrowserPlugin(BrowserPlugin):
    """A plugin for managing Stagehand AI-powered browser automation.

    Stagehand creates and manages the browser instance (local binary or Browserbase cloud).
    Playwright then connects to it via CDP, enabling both standard Playwright automation
    and AI-powered operations in the same crawling context.

    Only Chromium is supported because Stagehand relies on the Chrome DevTools Protocol.
    """

    AUTOMATION_LIBRARY = 'stagehand'

    def __init__(
        self,
        *,
        stagehand_options: StagehandOptions | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
        local_ready_timeout_s: float = 30.0,
    ) -> None:
        """Initialize a new instance.

        Args:
            stagehand_options: Stagehand-specific configuration. Defaults to
                ``StagehandOptions()`` if not provided.
            browser_new_context_options: Options passed to Playwright's
                ``browser.new_context`` after connecting via CDP. Refer to the
                Playwright documentation for available options:
                https://playwright.dev/python/docs/api/class-browser#browser-new-context.
            max_open_pages_per_browser: Maximum number of pages that can be open per browser.
            local_ready_timeout_s: Seconds to wait for the local Stagehand binary to
                become ready. Only relevant when ``env='LOCAL'``.
        """
        opts = stagehand_options or StagehandOptions()
        config = service_locator.get_configuration()

        self._opts = opts
        self._browser_new_context_options = browser_new_context_options or {}
        self._max_open_pages_per_browser = max_open_pages_per_browser

        # headless comes from Configuration, same as PlaywrightBrowserPlugin.
        # chrome_path is resolved lazily in __aenter__ once Playwright is available.
        self._headless = config.headless
        self._chrome_path: str | None = config.default_browser_path

        is_local = opts.env == 'LOCAL'
        self._stagehand_init_kwargs: dict[str, Any] = {
            'server': 'local' if is_local else 'remote',
            'local_headless': self._headless,
            'local_ready_timeout_s': local_ready_timeout_s,
        }
        if is_local:
            self._stagehand_init_kwargs['model_api_key'] = opts.api_key
        else:
            self._stagehand_init_kwargs['browserbase_api_key'] = opts.api_key
            self._stagehand_init_kwargs['browserbase_project_id'] = opts.project_id

        # AsyncStagehand is created lazily in __aenter__ so that chrome_path
        # can be resolved from playwright.chromium.executable_path if not set.
        self._stagehand_context_manager: AsyncStagehand | None = None
        self._stagehand_client: AsyncStagehand | None = None

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
        return 'chromium'

    @property
    @override
    def browser_launch_options(self) -> Mapping[str, Any]:
        """Return an empty mapping.

        Browser launch is managed by Stagehand, not Playwright directly.
        """
        return {}

    @property
    @override
    def browser_new_context_options(self) -> Mapping[str, Any]:
        """Return the options for the ``browser.new_context`` method.

        These options are passed to Playwright's ``browser.new_context`` after
        connecting to the Stagehand-managed browser via CDP. Refer to the Playwright
        documentation for available options:
        https://playwright.dev/python/docs/api/class-browser#browser-new-context.
        """
        return self._browser_new_context_options

    @property
    @override
    def max_open_pages_per_browser(self) -> int:
        return self._max_open_pages_per_browser

    @override
    async def __aenter__(self) -> StagehandBrowserPlugin:
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        self._playwright = await self._playwright_context_manager.__aenter__()

        # Resolve Chromium path from Playwright's own installation when not set
        # explicitly via Configuration. The stagehand binary needs an explicit path.
        if self._chrome_path is None and self._opts.env == 'LOCAL':
            self._chrome_path = self._playwright.chromium.executable_path
            self._stagehand_init_kwargs['local_chrome_path'] = self._chrome_path
            logger.debug(f'Resolved Chromium path from Playwright: {self._chrome_path}')

        self._stagehand_context_manager = AsyncStagehand(**self._stagehand_init_kwargs)
        self._stagehand_client = await self._stagehand_context_manager.__aenter__()

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

        if self._stagehand_context_manager is not None:
            await self._stagehand_context_manager.__aexit__(exc_type, exc_value, exc_traceback)

        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)

        self._stagehand_context_manager = None
        self._playwright_context_manager = async_playwright()
        self._stagehand_client = None
        self._playwright = None
        self._active = False

    @override
    @ensure_context
    async def new_browser(self) -> BrowserController:
        if not self._playwright or not self._stagehand_client:
            raise RuntimeError(f'{self.__class__.__name__} is not initialized.')

        session = await self._stagehand_client.sessions.start(**self._build_session_kwargs())

        cdp_url = session.data.cdp_url
        if not cdp_url:
            raise RuntimeError(
                f'No cdp_url returned from Stagehand (env={self._opts.env!r}). '
                'Cannot connect Playwright to the browser.'
            )

        browser = await self._playwright.chromium.connect_over_cdp(cdp_url)

        return StagehandBrowserController(
            browser,
            session,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
        )

    def _build_session_kwargs(self) -> dict[str, Any]:
        """Build keyword arguments for ``sessions.start``."""
        opts = self._opts

        if opts.env == 'BROWSERBASE':
            browser_param: dict[str, Any] = {'type': 'browserbase'}
        else:
            launch_options: dict[str, Any] = {'headless': self._headless}
            browser_param = {
                'type': 'local',
                'launchOptions': launch_options,
            }  # , 'local_chrome_path': self._chrome_path}

        kwargs: dict[str, Any] = {
            'model_name': opts.model,
            'browser': browser_param,
            'verbose': opts.verbose,
            'self_heal': opts.self_heal,
        }

        if opts.dom_settle_timeout_ms is not None:
            kwargs['dom_settle_timeout_ms'] = opts.dom_settle_timeout_ms
        if opts.system_prompt is not None:
            kwargs['system_prompt'] = opts.system_prompt

        return kwargs
