# Inspiration: https://github.com/apify/crawlee/blob/v3.10.0/packages/browser-pool/src/playwright/playwright-browser.ts

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from crawlee.browsers.base_browser import BaseBrowser

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, Page

    from crawlee.events import EventManager


class PlaywrightBrowser(BaseBrowser):
    """A Playwright browser implementation that wraps a single browser context."""

    def __init__(
        self,
        *,
        browser_context: BrowserContext,
        event_manager: EventManager,
    ) -> None:
        self._browser_context = browser_context
        self._event_manager = event_manager
        self._is_connected = True

        # Register an event handler for when the browser context is closed
        self._browser_context.on('close', self._on_browser_close)

    @property
    def is_connected(self) -> bool:
        """Check if the browser context is still connected."""
        return self._is_connected

    @property
    def contexts(self) -> Sequence[BrowserContext]:
        """Return a list of browser contexts (in this case, just one)."""
        return [self._browser_context]

    async def close(self) -> None:
        """Close the browser context."""
        await self._browser_context.close()

    async def new_page(self, *args: Any, **kwargs: Any) -> Page:
        """Create a new page in the browser context."""
        return await self._browser_context.new_page(*args, **kwargs)

    async def new_context(self) -> None:
        """Prevent creating a new context explicitly; raise an error instead."""
        raise NotImplementedError('Function `new_context()` is not available in this implementation.')

    async def new_browser_cdp_session(self) -> None:
        """Prevent creating a new CDP session; raise an error instead."""
        raise NotImplementedError('Function `new_browser_cdp_session()` is not available in this implementation.')

    async def start_tracing(self) -> None:
        """Prevent starting tracing; raise an error instead."""
        raise NotImplementedError('Function `start_tracing()` is not available in this implementation.')

    async def stop_tracing(self) -> None:
        """Prevent stopping tracing; raise an error instead."""
        raise NotImplementedError('Function `stop_tracing()` is not available in this implementation.')

    def _on_browser_close(self) -> None:
        """Handle the browser context's close event."""
        self._is_connected = False
        self._event_manager.emit(event='disconnected', event_data={})
