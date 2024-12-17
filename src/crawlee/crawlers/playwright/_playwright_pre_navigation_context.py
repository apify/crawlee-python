from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from playwright.async_api import Page


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightPreNavigationContext(BasicCrawlingContext):
    """Context used by PlaywrightCrawler.

    It Provides access to the `Page` object for the current browser page.
    """

    page: Page
    """The Playwright `Page` object for the current page."""
