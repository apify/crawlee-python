from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from crawlee import HttpHeaders
from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.crawlers import AbstractHttpParser, ParsedHttpCrawlingContext, PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from playwright.async_api import Page, Response
    from typing_extensions import Self

    from crawlee.crawlers._playwright._types import BlockRequestsFunction


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
    """The crawling context used by `AdaptivePlaywrightCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

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
        """Locate element by css selector and return `None` once it is found.

        If element is not found within timeout, `TimeoutError` is raised.

        Args:
            selector: Css selector to be used to locate specific element on page.
            timeout: Timeout that defines how long the function wait for the selector to appear.
        """
        if await self._static_parser.select(await self.parse_with_static_parser(), selector):
            return
        await self.page.locator(selector).wait_for(timeout=timeout.total_seconds() * 1000)

    async def query_selector_one(
        self, selector: str, timeout: timedelta = timedelta(seconds=5)
    ) -> TStaticSelectResult | None:
        """Locate element by css selector and return first element found.

        If element is not found within timeout, `TimeoutError` is raised.

        Args:
            selector: Css selector to be used to locate specific element on page.
            timeout: Timeout that defines how long the function wait for the selector to appear.

        Returns:
            Result of used static parser `select` method.
        """
        if matches := await self.query_selector_all(selector=selector, timeout=timeout):
            return matches[0]
        return None

    async def query_selector_all(
        self, selector: str, timeout: timedelta = timedelta(seconds=5)
    ) -> Sequence[TStaticSelectResult]:
        """Locate element by css selector and return all elements found.

        If element is not found within timeout, `TimeoutError` is raised.

        Args:
            selector: Css selector to be used to locate specific element on page.
            timeout: Timeout that defines how long the function wait for the selector to appear.

        Returns:
            List of results of used static parser `select` method.
        """
        if static_content := await self._static_parser.select(await self.parse_with_static_parser(), selector):
            # Selector found in static content.
            return static_content

        locator = self.page.locator(selector)
        try:
            await locator.wait_for(timeout=timeout.total_seconds() * 1000)
        except PlaywrightTimeoutError:
            # Selector not found at all.
            return ()

        parsed_selector = await self._static_parser.select(
            await self._static_parser.parse_text(await locator.evaluate('el => el.outerHTML')), selector
        )
        if parsed_selector is not None:
            # Selector found by browser after some wait time and selected by static parser.
            return parsed_selector

        # Selector found by browser after some wait time, but could not be selected by static parser.
        raise AdaptiveContextError(
            'Element exists on the page and Playwright was able to locate it, but the static content parser of selected'
            'static crawler does support such selector.'
        )

    async def parse_with_static_parser(
        self, selector: str | None = None, timeout: timedelta = timedelta(seconds=5)
    ) -> TStaticParseResult:
        """Parse whole page with static parser. If `selector` argument is used, wait for selector first.

        If element is not found within timeout, TimeoutError is raised.

        Args:
            selector: css selector to be used to locate specific element on page.
            timeout: timeout that defines how long the function wait for the selector to appear.

        Returns:
            Result of used static parser `parse_text` method.
        """
        if selector:
            await self.wait_for_selector(selector, timeout)
        if self._page:
            return await self._static_parser.parse_text(await self.page.content())
        return self.parsed_content

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
        # block_requests is useful only on pre-navigation contexts. It is useless here.
        context_kwargs.pop('block_requests')
        return cls(
            parsed_content=await parser.parse(http_response),
            http_response=http_response,
            _static_parser=parser,
            **context_kwargs,
        )


@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightPreNavCrawlingContext(BasicCrawlingContext):
    """This is just wrapper around BasicCrawlingContext or AdaptivePlaywrightCrawlingContext.

    Trying to access `page` on this context will raise AdaptiveContextError if wrapped context is BasicCrawlingContext.
    """

    _page: Page | None = None
    block_requests: BlockRequestsFunction | None = None
    """Blocks network requests matching specified URL patterns."""

    @property
    def page(self) -> Page:
        """The Playwright `Page` object for the current page.

        Raises `AdaptiveContextError` if accessed during static crawling.
        """
        if self._page is not None:
            return self._page
        raise AdaptiveContextError(
            'Page was crawled with static sub crawler and not with crawled with PlaywrightCrawler. For Playwright only '
            'hooks please use `playwright_only`=True when registering the hook. '
            'For example: @crawler.pre_navigation_hook(playwright_only=True)'
        )

    @classmethod
    def from_pre_navigation_context(cls, context: BasicCrawlingContext) -> Self:
        """Convenience constructor that creates new context from existing pre navigation contexts."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        context_kwargs['_page'] = context_kwargs.pop('page', None)

        # For static sub crawler replace block requests by function doing nothing.
        async def dummy_block_requests(
            url_patterns: list[str] | None = None,  # noqa:ARG001
            extra_url_patterns: list[str] | None = None,  # noqa:ARG001
        ) -> None:
            return

        context_kwargs['block_requests'] = context_kwargs.pop('block_requests', dummy_block_requests)
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
        # Used http protocol version cannot be obtained from `Response` and has to be passed as additional argument.
        http_version = protocol
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)
