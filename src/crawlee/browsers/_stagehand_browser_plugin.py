from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from playwright.async_api import Playwright, async_playwright
from stagehand import AsyncStagehand
from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group

from ._browser_plugin import BrowserPlugin
from ._stagehand_browser_controller import StagehandBrowserController
from ._types import StagehandOptions

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path
    from types import TracebackType

    from ._browser_controller import BrowserController
    from ._types import BrowserType


logger = getLogger(__name__)


@docs_group('Browser management')
class StagehandBrowserPlugin(BrowserPlugin):
    """A plugin for managing Stagehand AI-powered browser automation.

    It acts as a factory for creating `StagehandBrowserController` instances and manages the
    lifecycle of the shared `AsyncStagehand` REST client and the Playwright context. Depending
    on the configured environment, the browser runs locally via a bundled Chromium binary
    (``env='LOCAL'``) or in the Browserbase cloud (``env='BROWSERBASE'``). Playwright connects
    to the running browser via CDP, so only Chromium is supported.

    Not all Playwright browser and context options are supported — only those accepted by
    Stagehand's ``BrowserLaunchOptions``. Because Stagehand creates the browser and its context
    together in a single ``sessions.start()`` call, both ``browser_launch_options`` and
    ``browser_new_context_options`` are merged into one set of options applied at session start.
    """

    AUTOMATION_LIBRARY = 'stagehand'

    def __init__(
        self,
        *,
        user_data_dir: str | Path | None = None,
        stagehand_options: StagehandOptions | None = None,
        browser_launch_options: dict[str, Any] | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
    ) -> None:
        """Initialize a new instance.

        Args:
            user_data_dir: Path to a user data directory, which stores browser session data like
                cookies and local storage.
            stagehand_options: Stagehand-specific configuration (model, API key, env, etc.).
            browser_launch_options: Keyword arguments passed to Stagehand's ``BrowserLaunchOptions``
                on session start. Supported keys are a subset of Playwright's ``browser_type.launch``
                options. These take priority over ``browser_new_context_options`` for shared keys.
            browser_new_context_options: Additional options merged with ``browser_launch_options``
                at lower priority. Subject to the same ``BrowserLaunchOptions`` constraints.
            max_open_pages_per_browser: The maximum number of pages that can be open in a single
                browser instance. Once reached, a new browser instance will be launched.
        """
        config = service_locator.get_configuration()

        self._max_open_pages_per_browser = max_open_pages_per_browser
        self.stagehand_options = stagehand_options or StagehandOptions()

        is_local = self.stagehand_options.env == 'LOCAL'

        # browser_launch_options take priority over browser_new_context_options for shared keys.
        self._browser_launch_options: dict[str, Any] = {
            'headless': config.headless,
            'chromium_sandbox': not config.disable_browser_sandbox,
            **(browser_new_context_options or {}),
            **(browser_launch_options or {}),
        }

        if config.default_browser_path:
            self._browser_launch_options.setdefault('executable_path', config.default_browser_path)

        if user_data_dir is not None:
            self._browser_launch_options['user_data_dir'] = str(user_data_dir)

        # Parameters for AsyncStagehand.
        self._stagehand_init_params: dict[str, Any] = {
            'server': 'local' if is_local else 'remote',
            'local_headless': self._browser_launch_options['headless'],
            'local_ready_timeout_s': self.stagehand_options.local_ready_timeout_s,
        }

        self._stagehand_init_params['model_api_key'] = self.stagehand_options.api_key

        if not is_local:
            self._stagehand_init_params['browserbase_api_key'] = self.stagehand_options.api_key
            self._stagehand_init_params['browserbase_project_id'] = self.stagehand_options.project_id

        self._stagehand_client: AsyncStagehand | None = None
        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None
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
        """Return the options passed to Stagehand's ``BrowserLaunchOptions`` on session start.

        These are a subset of Playwright's ``browser_type.launch`` options — only keys recognised
        by Stagehand's ``BrowserLaunchOptions`` take effect.
        """
        return self._browser_launch_options

    @property
    @override
    def browser_new_context_options(self) -> Mapping[str, Any]:
        """Return the browser context options passed to Stagehand's ``BrowserLaunchOptions``.

        Stagehand creates the browser and its context together in a single ``sessions.start()``
        call, so context-level options such as ``viewport`` and ``locale`` are part of
        ``BrowserLaunchOptions`` and share the same dictionary as ``browser_launch_options``.
        Pre-navigation hooks that modify these options before the first page will take effect,
        because session creation is deferred until the first ``new_page`` call.
        """
        return self._browser_launch_options

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

        if self.stagehand_options.env == 'LOCAL':
            if 'executable_path' not in self._browser_launch_options:
                chrome_path = self._playwright.chromium.executable_path
                self._browser_launch_options['executable_path'] = chrome_path
                logger.debug(f'Resolved Chromium path from Playwright: {chrome_path}')

            self._stagehand_init_params['local_chrome_path'] = self._browser_launch_options['executable_path']

        client = AsyncStagehand(**self._stagehand_init_params)
        await client.__aenter__()
        self._stagehand_client = client

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

        if self._stagehand_client is not None:
            await self._stagehand_client.__aexit__(exc_type, exc_value, exc_traceback)
            self._stagehand_client = None

        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)
        self._playwright_context_manager = async_playwright()
        self._playwright = None
        self._active = False

    @override
    @ensure_context
    async def new_browser(self) -> BrowserController:
        if not self._playwright or not self._stagehand_client:
            raise RuntimeError(f'{self.__class__.__name__} is not initialized.')

        return StagehandBrowserController(
            playwright=self._playwright,
            stagehand_client=self._stagehand_client,
            stagehand_options=self.stagehand_options,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
        )
