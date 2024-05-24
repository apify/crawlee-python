# Inspiration: https://github.com/apify/crawlee/blob/v3.10.0/packages/browser-pool/src/abstract-classes/browser-plugin.ts

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from types import TracebackType

    from playwright.async_api import Page


class BaseBrowserPlugin(ABC):
    """An abstract base class for browser plugins."""

    @property
    @abstractmethod
    def browser_type(self) -> str:
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
    async def get_new_page(self, *, page_options: Mapping) -> Page:
        """Get a new page in a browser.

        Args:
            page_options: Options to configure the new page.
        """
