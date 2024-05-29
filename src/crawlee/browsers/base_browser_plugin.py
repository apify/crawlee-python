# Inspiration: https://github.com/apify/crawlee/blob/v3.10.0/packages/browser-pool/src/abstract-classes/browser-plugin.ts

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from types import TracebackType

    from playwright.async_api import Browser, Page


class BaseBrowserPlugin(ABC):
    """An abstract base class for browser plugins.

    Browser plugins act as wrappers around browser automation tools like Playwright,
    providing a unified interface for interacting with browsers.
    """

    @property
    @abstractmethod
    def browser(self) -> Browser | None:
        """Return the browser instance."""

    @property
    @abstractmethod
    def browser_type(self) -> Literal['chromium', 'firefox', 'webkit']:
        """Return the browser type name."""

    @abstractmethod
    async def __aenter__(self) -> BaseBrowserPlugin:
        """Enter the context manager and initialize the browser plugin."""

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the browser plugin."""

    @abstractmethod
    async def new_page(self) -> Page:
        """Get a new page in a browser."""
