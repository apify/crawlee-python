from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from crawlee import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.crawlers import BeautifulSoupCrawlingContext, BeautifulSoupParserType, PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from playwright.async_api import Page, Response
    from typing_extensions import Self


@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightCrawlingContext(BeautifulSoupCrawlingContext):
    _response: Response | None = None
    _infinite_scroll: Callable[[], Awaitable[None]] | None = None
    _page : Page | None = None
    # TODO: UseStateFunction

    @property
    def page(self) -> Page:
        if not self._page:
            raise RuntimeError('Page was not crawled with PlaywrightCrawler')
        return self._page

    @property
    def infinite_scroll(self) -> Callable[[], Awaitable[None]]:
        if not self._infinite_scroll:
            raise RuntimeError('Page was not crawled with PlaywrightCrawler')
        return self._infinite_scroll

    @property
    def response(self) -> Response:
        if not self._response:
            raise RuntimeError('Page was not crawled with PlaywrightCrawler')
        return self._response

    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: BeautifulSoupCrawlingContext) -> Self:
        """Convenience constructor that creates new context from existing `BeautifulSoupCrawlingContext`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    @classmethod
    async def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext,
                                            beautiful_soup_parser_type: BeautifulSoupParserType | None) -> Self:
        """Convenience constructor that creates new context from existing `PlaywrightCrawlingContext`."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Remove playwright specific attributes and pass them as private instead to be available as property.
        context_kwargs['_response'] = context_kwargs.pop('response')
        context_kwargs['_page'] = context_kwargs.pop('page')
        context_kwargs['_infinite_scroll'] = context_kwargs.pop('infinite_scroll')
        http_response = await _HttpResponse.from_playwright_response(context.response)
        return cls(parsed_content= BeautifulSoup(http_response.read(), features=beautiful_soup_parser_type),
                   http_response = http_response,
                   **context_kwargs)


@dataclass(frozen=True)
class _HttpResponse:
    http_version : str
    status_code : int
    headers: HttpHeaders
    _content: bytes

    def read(self) -> bytes:
        return self._content

    @classmethod
    async def from_playwright_response(cls, response: Response) -> Self:
        headers = HttpHeaders(response.headers)
        status_code = response.status
        # Can't find this anywhere in PlayWright, but some headers can include information about protocol.
        # In firefox for example: 'x-firefox-spdy'
        # Might be also obtained by executing JS code in browser: performance.getEntries()[0].nextHopProtocol
        # Response headers capitalization not respecting http1.1 Pascal case. Always lower case in PlayWright.
        http_version = 'TODO'
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)
