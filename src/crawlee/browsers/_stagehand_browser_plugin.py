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

    Stagehand creates and manages the browser instance (local binary or Browserbase cloud).
    Playwright then connects to it via CDP, enabling both standard Playwright automation
    and AI-powered operations in the same crawling context.

    Only Chromium is supported because Stagehand relies on the Chrome DevTools Protocol.
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
            user_data_dir: Path to a User Data Directory, which stores browser session data like cookies and local
                storage.
            stagehand_options: Stagehand-specific configuration (model, API key, env, etc.).
            browser_launch_options: Keyword arguments for browser launch. Supported options are
                a subset of Playwright's ``browser_type.launch`` options that map to Stagehand's
                ``BrowserLaunchOptions``. Unsupported keys are logged as warnings and ignored.
            browser_new_context_options: Keyword arguments for browser context creation.
                Options that map to Stagehand's ``BrowserLaunchOptions`` are merged with
                ``browser_launch_options``. Unsupported keys are logged as warnings and ignored.
            max_open_pages_per_browser: Maximum number of pages that can be open per browser.
        """
        config = service_locator.get_configuration()

        self._max_open_pages_per_browser = max_open_pages_per_browser

        self.stagehand_options = stagehand_options or StagehandOptions()
        self._browser_new_context_options = browser_new_context_options or {}

        is_local = self.stagehand_options.env == 'LOCAL'

        self._base_launch_options: dict[str, Any] = {
            'headless': config.headless,
            'chromium_sandbox': not config.disable_browser_sandbox,
        }
        if config.default_browser_path:
            self._base_launch_options['executable_path'] = config.default_browser_path

        self._base_launch_options = {**self._base_launch_options, **(browser_launch_options or {})}

        self._stagehand_init_kwargs: dict[str, Any] = {
            'server': 'local' if is_local else 'remote',
            'local_headless': self._base_launch_options.get('headless', config.headless),
            'local_ready_timeout_s': self.stagehand_options.local_ready_timeout_s,
            'user_data_dir': str(user_data_dir) if user_data_dir else None,
        }
        if is_local:
            self._stagehand_init_kwargs['model_api_key'] = self.stagehand_options.api_key
        else:
            self._stagehand_init_kwargs['browserbase_api_key'] = self.stagehand_options.api_key
            self._stagehand_init_kwargs['browserbase_project_id'] = self.stagehand_options.project_id

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
        return self._base_launch_options

    @property
    @override
    def browser_new_context_options(self) -> Mapping[str, Any]:
        return {}

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

        # Resolve Chromium path for LOCAL mode.
        if self.stagehand_options.env == 'LOCAL':
            if 'executable_path' not in self._base_launch_options:
                chrome_path = self._playwright.chromium.executable_path
                self._base_launch_options['executable_path'] = chrome_path
                logger.debug(f'Resolved Chromium path from Playwright: {chrome_path}')

            self._stagehand_init_kwargs['local_chrome_path'] = self._base_launch_options['executable_path']

        client = AsyncStagehand(**self._stagehand_init_kwargs)
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
            base_launch_options=self._base_launch_options,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
        )
