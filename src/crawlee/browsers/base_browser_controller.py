# Inspiration: https://github.com/apify/crawlee/blob/v3.10.1/packages/browser-pool/src/abstract-classes/browser-controller.ts

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime, timedelta

    from playwright.async_api import Page

    from crawlee.proxy_configuration import ProxyInfo


class BaseBrowserController(ABC):
    """An abstract class for managing browser instance and their pages."""

    AUTOMATION_LIBRARY: str | None = None
    """The name of the automation library that the controller is using."""

    @property
    @abstractmethod
    def pages(self) -> list[Page]:
        """Return the list of opened pages."""

    @property
    @abstractmethod
    def pages_count(self) -> int:
        """Returns the number of currently open pages."""

    @property
    @abstractmethod
    def last_page_opened_at(self) -> datetime:
        """Return the time when the last page was opened."""

    @property
    @abstractmethod
    def idle_time(self) -> timedelta:
        """Return the idle time of the browser controller."""

    @property
    @abstractmethod
    def has_free_capacity(self) -> bool:
        """Return if the browser has free capacity to open a new page."""

    @property
    @abstractmethod
    def is_browser_connected(self) -> bool:
        """Return if the browser is closed."""

    @abstractmethod
    async def new_page(
        self,
        page_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> Page:
        """Create a new page with the given context options.

        Args:
            page_options: Options to configure the new page.
            proxy_info: The proxy configuration to use for the new page.

        Returns:
            Page: The newly created page.

        Raises:
            ValueError: If the browser has reached the maximum number of open pages.
        """

    @abstractmethod
    async def close(self, *, force: bool = False) -> None:
        """Close the browser.

        Args:
            force: Whether to force close all open pages before closing the browser.

        Raises:
            ValueError: If there are still open pages when trying to close the browser.
        """
