from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction
from crawlee._utils.docs import docs_group
from crawlee.http_crawler import HttpCrawlingResult

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


@dataclass(frozen=True)
@docs_group('Data structures')
class BeautifulSoupCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """The crawling context used by the `BeautifulSoupCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    soup: BeautifulSoup
    """The `BeautifulSoup` object for the current page."""

    enqueue_links: EnqueueLinksFunction
    """The BeautifulSoup `EnqueueLinksFunction` implementation."""
