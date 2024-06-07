# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/abstract-classes/browser-plugin.ts

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from crawlee.browsers.base_browser_controller import BaseBrowserController


class BaseBrowserPlugin(ABC):
    """An abstract base class for browser plugins.

    Browser plugins act as wrappers around browser automation tools like Playwright,
    providing a unified interface for interacting with browsers.
    """

    AUTOMATION_LIBRARY: str | None = None
    """The name of the automation library that the plugin is managing."""

    @property
    @abstractmethod
    def browser_type(self) -> Literal['chromium', 'firefox', 'webkit']:
        """Return the browser type name."""

    @property
    @abstractmethod
    def browser_options(self) -> Mapping[str, Any]:
        """Return the options for a new browser."""

    @property
    @abstractmethod
    def page_options(self) -> Mapping[str, Any]:
        """Return the options for a new page."""

    @property
    @abstractmethod
    def max_open_pages_per_browser(self) -> int:
        """Return the maximum number of pages that can be opened in a single browser."""

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
    async def new_browser(self) -> BaseBrowserController:
        """Create a new browser instance.

        Returns:
            A new browser instance wrapped in a controller.
        """
