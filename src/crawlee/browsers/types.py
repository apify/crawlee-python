from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    from playwright.async_api import Page


@dataclass
class CrawleePage:
    """Represents a page object within a browser, with additional metadata for tracking and management."""

    id: str
    browser_type: Literal['chromium', 'firefox', 'webkit']
    page: Page


class PageOptions(TypedDict):
    """Options for creating a new page."""


class BrowserOptions(TypedDict):
    """Options for launching a new browser."""


#
