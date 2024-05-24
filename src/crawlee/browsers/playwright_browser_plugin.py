from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Literal, override

from playwright.async_api import async_playwright

from crawlee.browsers.base_browser_plugin import BaseBrowserPlugin
from crawlee.browsers.types import BrowserOptions, PageOptions

if TYPE_CHECKING:
    from types import TracebackType

    from playwright.async_api import Page

    from crawlee.events import EventManager

logger = getLogger(__name__)


class PlaywrightBrowserPlugin(BaseBrowserPlugin):
    """A Playwright browser plugin that manages browser instances."""

    def __init__(
        self,
        *,
        browser_type: Literal['chromium', 'firefox', 'webkit'] = 'chromium',
        browser_options: BrowserOptions | None = None,
        event_manager: EventManager | None = None,
    ) -> None:
        self._browser_type = browser_type
        self._browser_options = browser_options or BrowserOptions()
        self._event_manager = event_manager

        self._playwright_context_manager = async_playwright()
        self._playwright = None
        self._browser = None

    @property
    @override
    def browser_type(self) -> str:
        return self._browser_type

    @override
    async def __aenter__(self) -> PlaywrightBrowserPlugin:
        logger.info('Initializing Playwright browser plugin.')
        self._playwright = await self._playwright_context_manager.__aenter__()

        if self.browser_type == 'chromium':
            self._browser = await self._playwright.chromium.launch(**self._browser_options)
        elif self.browser_type == 'firefox':
            self._browser = await self._playwright.firefox.launch(**self._browser_options)
        elif self.browser_type == 'webkit':
            self._browser = await self._playwright.webkit.launch(**self._browser_options)
        else:
            raise ValueError(f'Invalid browser type: {self.browser_type}')

        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        logger.info('Closing Playwright browser plugin.')
        await self._browser.close()
        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)

    @override
    async def get_new_page(self, *, page_options: PageOptions) -> Page:
        if not self._browser:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return await self._browser.new_page(**page_options)

    # async def _launch(self, **kwargs: Any) -> Browser:
    #     use_incognito_pages = kwargs.pop('use_incognito_pages', False)

    #     # WebKit does not support --no-sandbox
    #     if self.browser_type_name == 'webkit':
    #         kwargs['args'] = [arg for arg in kwargs.get('args', []) if arg != '--no-sandbox']

    #     if use_incognito_pages:
    #         browser = await self._browser_type.launch(**kwargs)
    #     else:
    #         user_data_dir = kwargs.pop('user_data_dir', None)
    #         browser_context = await self._browser_type.launch_persistent_context(user_data_dir, **kwargs)

    #         browser = PlaywrightBrowser(
    #             browser_context=browser_context,
    #             event_manager=self._event_manager,
    #         )

    #     return browser

    # def _create_controller(self) -> PlaywrightBrowserController:
    #     return PlaywrightBrowserController(self._browser_type)
