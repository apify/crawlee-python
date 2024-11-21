from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction
from crawlee._utils.docs import docs_group
from crawlee.http_crawler import HttpCrawlingResult

if TYPE_CHECKING:
    from parsel import Selector


@dataclass(frozen=True)
@docs_group('Data structures')
class ParselCrawlingContext(HttpCrawlingResult, BasicCrawlingContext):
    """The crawling context used by the `ParselCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    selector: Selector
    """The Parsel `Selector` object for the current page."""

    enqueue_links: EnqueueLinksFunction
    """The Parsel `EnqueueLinksFunction` implementation."""
