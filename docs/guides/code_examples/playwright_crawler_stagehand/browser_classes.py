from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from stagehand.context import StagehandContext
from typing_extensions import override

from crawlee.browsers import (
    PlaywrightBrowserController,
    PlaywrightBrowserPlugin,
    PlaywrightPersistentBrowser,
)

from .support_classes import CrawleeStagehandPage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from playwright.async_api import Page
    from stagehand import Stagehand

    from crawlee.proxy_configuration import ProxyInfo


class StagehandBrowserController(PlaywrightBrowserController):
    @override
    def __init__(
        self, browser: PlaywrightPersistentBrowser, stagehand: Stagehand, **kwargs: Any
    ) -> None:
        # Initialize with browser context instead of browser instance
        super().__init__(browser, **kwargs)

        self._stagehand = stagehand
        self._stagehand_context: StagehandContext | None = None

    @override
    async def new_page(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        # Initialize browser context if not already done
        if not self._browser_context:
            self._browser_context = await self._create_browser_context(
                browser_new_context_options=browser_new_context_options,
                proxy_info=proxy_info,
            )

        # Initialize Stagehand context if not already done
        if not self._stagehand_context:
            self._stagehand_context = await StagehandContext.init(
                self._browser_context, self._stagehand
            )

        # Create a new page using Stagehand context
        page = await self._stagehand_context.new_page()

        pw_page = page._page  # noqa: SLF001

        # Handle page close event
        pw_page.on(event='close', f=self._on_page_close)

        # Update internal state
        self._pages.append(pw_page)
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

    @override
    async def new_browser(self) -> StagehandBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        browser = PlaywrightPersistentBrowser(
            # Stagehand can run only on a Chromium-based browser.
            self._playwright.chromium,
            self._user_data_dir,
            self._browser_launch_options,
        )

        # Return custom controller with Stagehand
        return StagehandBrowserController(
            browser=browser,
            stagehand=self._stagehand,
            header_generator=None,
            fingerprint_generator=None,
        )
