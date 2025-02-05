from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Generic

from typing_extensions import TypeVar

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

    from crawlee.crawlers._playwright._types import BlockRequestsFunction


class AdaptiveContextError(RuntimeError):
    pass


TStaticParseResult = TypeVar('TStaticParseResult')


@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightCrawlingContext(Generic[TStaticParseResult], ParsedHttpCrawlingContext[TStaticParseResult]):
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

    @classmethod
    def from_parsed_http_crawling_context(
        cls, context: ParsedHttpCrawlingContext[TStaticParseResult]
    ) -> AdaptivePlaywrightCrawlingContext[TStaticParseResult]:
        """Convenience constructor that creates new context from existing `ParsedHttpCrawlingContext`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    @classmethod
    async def from_playwright_crawling_context(
        cls, context: PlaywrightCrawlingContext, parser: AbstractHttpParser[TStaticParseResult]
    ) -> Self:
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
