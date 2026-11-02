from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from playwright.async_api import Page

BrowserType = Literal['chromium', 'firefox', 'webkit', 'chrome']


@dataclass
class CrawleePage:
    """Represents a page object within a browser, with additional metadata for tracking and management."""

    id: str
    browser_type: BrowserType
    page: Page
    context_headers: dict[str, str] = field(default_factory=dict)
    """Extra HTTP headers that the page's browser context sends with every request."""
