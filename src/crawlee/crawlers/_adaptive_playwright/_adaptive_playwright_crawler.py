from dataclasses import dataclass
from typing import Callable, Awaitable

from bs4 import BeautifulSoup
from playwright.async_api import Page, Response

from crawlee._types import EnqueueLinksFunction
from crawlee._utils.docs import docs_group
from crawlee.crawlers import BeautifulSoupCrawler, PlaywrightCrawler, ContextPipeline, AbstractHttpCrawler, \
    ParsedHttpCrawlingContext, PlaywrightCrawlingContext
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser


@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptiveCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup], PlaywrightCrawlingContext):
    _page: Page
    _response: Response
    _enqueue_links: EnqueueLinksFunction
    _infinite_scroll: Callable[[], Awaitable[None]]

    @property
    def page(self) -> Page:
        """The Playwright `Page` object for the current page."""
        return self._page

    @property
    def response(self) -> Response:
        """The Playwright `Response` object containing the response details for the current URL."""
        return self._response

    @property
    def enqueue_links(self) -> EnqueueLinksFunction:
        """The Playwright `EnqueueLinksFunction` implementation."""
        return self._enqueue_links

    @property
    def infinite_scroll(self) ->  Callable[[], Awaitable[None]]:
        """A function to perform infinite scrolling on the page. This scrolls to the bottom, triggering
        the loading of additional content if present."""
        return self._infinite_scroll

class AdaptivePlaywrightCrawler(AbstractHttpCrawler[AdaptiveCrawlingContext, BeautifulSoup], PlaywrightCrawler):

    def __init__(self):
        context_pipeline = ContextPipeline().compose(self._open_page).compose(self._navigate).compose(
            self._handle_blocked_request)
        super().__init__(parser=BeautifulSoupParser(), _context_pipeline=context_pipeline)
        self._context_pipeline = ContextPipeline().compose(self._open_page).compose(self._navigate)

    def _decide_crawler_type(self):

