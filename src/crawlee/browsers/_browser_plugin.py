# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/abstract-classes/browser-plugin.ts

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from crawlee.browsers._browser_controller import BrowserController
    from crawlee.browsers._types import BrowserType


class BrowserPlugin(ABC):
    """An abstract base class for browser plugins.

    Browser plugins act as wrappers around browser automation tools like Playwright,
    providing a unified interface for interacting with browsers.
    """

    AUTOMATION_LIBRARY: str | None = None
    """The name of the automation library that the plugin is managing."""

    @property
    @abstractmethod
    def active(self) -> bool:
        """Indicate whether the context is active."""

    @property
    @abstractmethod
    def browser_type(self) -> BrowserType:
        """Return the browser type name."""

    @property
    @abstractmethod
    def browser_launch_options(self) -> Mapping[str, Any]:
        """Return the options for the `browser.launch` method.

        Keyword arguments to pass to the browser launch method. These options are provided directly to Playwright's
        `browser_type.launch` method. For more details, refer to the Playwright documentation:
         https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
        """

    @property
    @abstractmethod
    def browser_new_context_options(self) -> Mapping[str, Any]:
        """Return the options for the `browser.new_context` method.

        Keyword arguments to pass to the browser new context method. These options are provided directly to Playwright's
        `browser.new_context` method. For more details, refer to the Playwright documentation:
        https://playwright.dev/python/docs/api/class-browser#browser-new-context.
        """

    @property
    @abstractmethod
    def max_open_pages_per_browser(self) -> int:
        """Return the maximum number of pages that can be opened in a single browser."""

    @abstractmethod
    async def __aenter__(self) -> BrowserPlugin:
        """Enter the context manager and initialize the browser plugin.

        Raises:
            RuntimeError: If the context manager is already active.
        """

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the browser plugin.

        Raises:
            RuntimeError: If the context manager is not active.
        """

    @abstractmethod
    async def new_browser(self) -> BrowserController:
        """Create a new browser instance.

        Returns:
            A new browser instance wrapped in a controller.
        """
