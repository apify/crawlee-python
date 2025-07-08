from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from stagehand.browser import cleanup_browser_resources, connect_local_browser
from typing_extensions import override

from crawlee.browsers import (
    PlaywrightBrowserController,
    PlaywrightBrowserPlugin,
)

from .support_classes import CrawleeStagehandPage

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from playwright.async_api import Browser, BrowserContext, Page
    from stagehand import Stagehand
    from stagehand.context import StagehandContext

    from crawlee.proxy_configuration import ProxyInfo


class StagehandBrowserController(PlaywrightBrowserController):
    @override
    def __init__(
        self, browser: BrowserContext, stagehand_context: StagehandContext, **kwargs: Any
    ) -> None:
        # Initialize with browser context instead of browser instance
        super().__init__(cast('Browser', browser), **kwargs)

        self._browser_context = browser
        self._stagehand_context = stagehand_context

    @override
    async def new_page(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        # Create new page through StagehandContext instead of browser context
        page = await self._stagehand_context.new_page()

        # Track the page for proper lifecycle management
        self._pages.append(page)
        self._last_page_opened_at = datetime.now(timezone.utc)

        self._total_opened_pages += 1

        # Wrap StagehandPage to provide Playwright Page interface
        return cast('Page', CrawleeStagehandPage(page))


class StagehandPlugin(PlaywrightBrowserPlugin):
    """Browser plugin that integrates Stagehand with Crawlee's browser management."""

    @override
    def __init__(self, stagehand: Stagehand, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self._stagehand = stagehand
        self._temp_user_data_dir = None

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await super().__aexit__(exc_type, exc_value, exc_traceback)
        # Clean up temporary browser resources created by Stagehand
        if self._temp_user_data_dir:
            await cleanup_browser_resources(  # type: ignore[unreachable]
                None, None, None, self._temp_user_data_dir, self._stagehand.logger
            )

    @override
    async def new_browser(self) -> StagehandBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        # Connect to local browser with Stagehand capabilities
        connect_result = await connect_local_browser(
            self._playwright,
            self._browser_launch_options,
            self._stagehand,
            self._stagehand.logger,
        )

        # Unpack the connection result
        (_, persist_context, stagehand_context, page, self._temp_user_data_dir) = (
            connect_result
        )

        # Close the initial page as we'll create new ones through the controller
        await page.close()

        # Return custom controller that uses StagehandContext
        return StagehandBrowserController(
            browser=persist_context,
            stagehand_context=stagehand_context,
            header_generator=None,
            fingerprint_generator=None,
        )
