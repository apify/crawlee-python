from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar

from crawlee import HttpHeaders
from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.crawlers import (
    AbstractHttpParser,
    ParsedHttpCrawlingContext,
    PlaywrightCrawlingContext,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from playwright.async_api import Page, Response
    from typing_extensions import Self


TStaticParseResult = TypeVar('TStaticParseResult')
TStaticSelectResult = TypeVar('TStaticSelectResult')


class AdaptiveContextError(RuntimeError):
    pass


@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightCrawlingContext(
    Generic[TStaticParseResult, TStaticSelectResult], ParsedHttpCrawlingContext[TStaticParseResult]
):
    _static_parser: AbstractHttpParser[TStaticParseResult, TStaticSelectResult]
    _response: Response | None = None
    _infinite_scroll: Callable[[], Awaitable[None]] | None = None
    _page: Page | None = None

    @property
    def page(self) -> Page:
        """The Playwright `Page` object for the current page.

        Raises `AdaptiveContextError` if accessed during static crawling.
        """
        if not self._page:
            raise AdaptiveContextError('Page was not crawled with PlaywrightCrawler.')
        return self._page

    @property
    def infinite_scroll(self) -> Callable[[], Awaitable[None]]:
        """A function to perform infinite scrolling on the page.

        This scrolls to the bottom, triggering the loading of additional content if present.
        Raises `AdaptiveContextError` if accessed during static crawling.
        """
        if not self._infinite_scroll:
            raise AdaptiveContextError('Page was not crawled with PlaywrightCrawler.')
        return self._infinite_scroll

    @property
    def response(self) -> Response:
        """The Playwright `Response` object containing the response details for the current URL.

        Raises `AdaptiveContextError` if accessed during static crawling.
        """
        if not self._response:
            raise AdaptiveContextError('Page was not crawled with PlaywrightCrawler.')
        return self._response

    async def wait_for_selector(self, selector: str, timeout: timedelta = timedelta(seconds=5)) -> None:
        """Locate element by css selector a return once it is found.

        If element is not found within timeout, TimeoutError is raised.

        Args:
            selector: css selector to be used to locate specific element on page.
            timeout: timeout that defines how long the function wait for the selector to appear.
        """
        if await self._static_parser.select(self.parsed_content, selector):
            return
        await self.page.locator(selector).wait_for(timeout=timeout.total_seconds() * 1000)

    async def query_selector(self, selector: str, timeout: timedelta = timedelta(seconds=5)) -> TStaticSelectResult:
        """Locate element by css selector a return it once it is found.

        If element is not found within timeout, TimeoutError is raised.

        Args:
            selector: css selector to be used to locate specific element on page.
            timeout: timeout that defines how long the function wait for the selector to appear.

        Returns:
            `TStaticSelectResult` which is result of used static parser `select` method.
        """
        static_content = await self._static_parser.select(self.parsed_content, selector)
        if static_content is not None:
            return static_content

        locator = self.page.locator(selector)
        await locator.wait_for(timeout=timeout.total_seconds() * 1000)

        parsed_selector = await self._static_parser.select(
            await self._static_parser.parse_text(await locator.evaluate('el => el.outerHTML')), selector
        )
        if parsed_selector is not None:
            return parsed_selector
        raise AdaptiveContextError('Used selector is not a valid static selector')

    async def parse_with_static_parser(
        self, selector: str | None, timeout: timedelta = timedelta(seconds=5)
    ) -> TStaticParseResult:
        """Parse whole page with static parser. If `selector` argument is used wait for selector first.

        If element is not found within timeout, TimeoutError is raised.

        Args:
            selector: css selector to be used to locate specific element on page.
            timeout: timeout that defines how long the function wait for the selector to appear.

        Returns:
            `TStaticParseResult` which is result of used static parser `parse_text` method.
        """
        if selector:
            await self.wait_for_selector(selector, timeout)
        return await self._static_parser.parse_text(await self.page.content())

    @classmethod
    def from_parsed_http_crawling_context(
        cls,
        context: ParsedHttpCrawlingContext[TStaticParseResult],
        parser: AbstractHttpParser[TStaticParseResult, TStaticSelectResult],
    ) -> AdaptivePlaywrightCrawlingContext[TStaticParseResult, TStaticSelectResult]:
        """Convenience constructor that creates new context from existing `ParsedHttpCrawlingContext`."""
        return cls(_static_parser=parser, **{field.name: getattr(context, field.name) for field in fields(context)})

    @classmethod
    async def from_playwright_crawling_context(
        cls, context: PlaywrightCrawlingContext, parser: AbstractHttpParser[TStaticParseResult, TStaticSelectResult]
    ) -> AdaptivePlaywrightCrawlingContext[TStaticParseResult, TStaticSelectResult]:
        """Convenience constructor that creates new context from existing `PlaywrightCrawlingContext`."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Remove playwright specific attributes and pass them as private instead to be available as property.
        context_kwargs['_response'] = context_kwargs.pop('response')
        context_kwargs['_page'] = context_kwargs.pop('page')
        context_kwargs['_infinite_scroll'] = context_kwargs.pop('infinite_scroll')
        # This might not be always available.
        protocol_guess = await context_kwargs['_page'].evaluate('() => performance.getEntries()[0].nextHopProtocol')
        http_response = await _PlaywrightHttpResponse.from_playwright_response(
            response=context.response, protocol=protocol_guess or ''
        )
        return cls(
            parsed_content=await parser.parse(http_response),
            http_response=http_response,
            _static_parser=parser,
            **context_kwargs,
        )


@dataclass(frozen=True)
class AdaptivePlaywrightPreNavCrawlingContext(BasicCrawlingContext):
    """This is just wrapper around BasicCrawlingContext or AdaptivePlaywrightCrawlingContext.

    Trying to access `page` on this context will raise AdaptiveContextError if wrapped context is BasicCrawlingContext.
    """

    _page: Page | None = None

    @property
    def page(self) -> Page:
        """The Playwright `Page` object for the current page.

        Raises `AdaptiveContextError` if accessed during static crawling.
        """
        if self._page is not None:
            return self._page
        raise AdaptiveContextError('Page is not crawled with PlaywrightCrawler.')

    @classmethod
    def from_pre_navigation_context(cls, context: BasicCrawlingContext) -> Self:
        """Convenience constructor that creates new context from existing pre navigation contexts."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        context_kwargs['_page'] = context_kwargs.pop('page', None)
        return cls(**context_kwargs)


@dataclass(frozen=True)
class _PlaywrightHttpResponse:
    """Wrapper class for playwright `Response` object to implement `HttpResponse` protocol."""

    http_version: str
    status_code: int
    headers: HttpHeaders
    _content: bytes

    def read(self) -> bytes:
        return self._content

    @classmethod
    async def from_playwright_response(cls, response: Response, protocol: str) -> Self:
        headers = HttpHeaders(response.headers)
        status_code = response.status
        # Can't find this anywhere in PlayWright, but some headers can include information about protocol.
        # In firefox for example: 'x-firefox-spdy'
        # Might be also obtained by executing JS code in browser: performance.getEntries()[0].nextHopProtocol
        # Response headers capitalization not respecting http1.1 Pascal case. Always lower case in PlayWright.
        http_version = protocol
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)
