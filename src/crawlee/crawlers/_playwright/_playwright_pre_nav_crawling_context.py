from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from playwright.async_api import Page

    from ._types import BlockRequestsFunction


@dataclass(frozen=True)
@docs_group('Data structures')
class PlaywrightPreNavCrawlingContext(BasicCrawlingContext):
    """The pre navigation crawling context used by the `PlaywrightCrawler`.

    It provides access to the `Page` object, before the navigation to the URL is performed.
    """

    page: Page
    """The Playwright `Page` object for the current page."""

    block_requests: BlockRequestsFunction
    """Blocks network requests matching specified URL patterns."""
