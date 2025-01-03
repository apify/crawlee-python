from __future__ import annotations

import logging
from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, Generic

from pydantic import ValidationError
from typing_extensions import NotRequired, TypeVar

from crawlee import EnqueueStrategy
from crawlee._request import BaseRequestData
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.crawlers._basic import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient

from ._http_crawling_context import HttpCrawlingContext, ParsedHttpCrawlingContext, TParseResult

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Iterable

    from typing_extensions import Unpack

    from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, EnqueueLinksKwargs

    from ._abstract_http_parser import AbstractHttpParser

TCrawlingContext = TypeVar('TCrawlingContext', bound=ParsedHttpCrawlingContext)


@docs_group('Data structures')
class HttpCrawlerOptions(Generic[TCrawlingContext], BasicCrawlerOptions[TCrawlingContext]):
    """Arguments for the `AbstractHttpCrawler` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    """

    additional_http_error_status_codes: NotRequired[Iterable[int]]
    """Additional HTTP status codes to treat as errors, triggering automatic retries when encountered."""

    ignore_http_error_status_codes: NotRequired[Iterable[int]]
    """HTTP status codes that are typically considered errors but should be treated as successful responses."""


@docs_group('Abstract classes')
class AbstractHttpCrawler(Generic[TCrawlingContext, TParseResult], BasicCrawler[TCrawlingContext], ABC):
    """A web crawler for performing HTTP requests.

    The `AbstractHttpCrawler` builds on top of the `BasicCrawler`, inheriting all its features. Additionally,
    it implements HTTP communication using HTTP clients. The class allows integration with any HTTP client
    that implements the `BaseHttpClient` interface, provided as an input parameter to the constructor.

    `AbstractHttpCrawler` is a generic class intended to be used with a specific parser for parsing HTTP responses
    and the expected type of `TCrawlingContext` available to the user function. Examples of specific versions include
    `BeautifulSoupCrawler`, `ParselCrawler`, and `HttpCrawler`.

    HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. For websites that
    require client-side JavaScript execution, consider using a browser-based crawler like the `PlaywrightCrawler`.
    """

    def __init__(
        self,
        *,
        parser: AbstractHttpParser[TParseResult],
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[TCrawlingContext]],
    ) -> None:
        self._parser = parser
        self._pre_navigation_hooks: list[Callable[[BasicCrawlingContext], Awaitable[None]]] = []

        kwargs.setdefault(
            'http_client',
            HttpxHttpClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
        )

        if '_context_pipeline' not in kwargs:
            raise ValueError(
                'Please pass in a `_context_pipeline`. You should use the '
                'AbstractHttpCrawler._create_static_content_crawler_pipeline() method to initialize it.'
            )

        kwargs.setdefault('_logger', logging.getLogger(__name__))
        super().__init__(**kwargs)

    def _create_static_content_crawler_pipeline(self) -> ContextPipeline[ParsedHttpCrawlingContext[TParseResult]]:
        """Create static content crawler context pipeline with expected pipeline steps."""
        return (
            ContextPipeline()
            .compose(self._execute_pre_navigation_hooks)
            .compose(self._make_http_request)
            .compose(self._parse_http_response)
            .compose(self._handle_blocked_request)
        )

    async def _execute_pre_navigation_hooks(
        self, context: BasicCrawlingContext
    ) -> AsyncGenerator[BasicCrawlingContext, None]:
        for hook in self._pre_navigation_hooks:
            await hook(context)
        yield context

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        """Parse HTTP response and create context enhanced by the parsing result and enqueue links function.

        Args:
            context: The current crawling context, that includes HTTP response.

        Yields:
            The original crawling context enhanced by the parsing result and enqueue links function.
        """
        parsed_content = await self._parser.parse(context.http_response)
        yield ParsedHttpCrawlingContext.from_http_crawling_context(
            context=context,
            parsed_content=parsed_content,
            enqueue_links=self._create_enqueue_links_function(context, parsed_content),
        )

    def _create_enqueue_links_function(
        self, context: HttpCrawlingContext, parsed_content: TParseResult
    ) -> EnqueueLinksFunction:
        """Create a callback function for extracting links from parsed content and enqueuing them to the crawl.

        Args:
            context: The current crawling context.
            parsed_content: The parsed http response.

        Returns:
            Awaitable that is used for extracting links from parsed content and enqueuing them to the crawl.
        """

        async def enqueue_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict[str, Any] | None = None,
            **kwargs: Unpack[EnqueueLinksKwargs],
        ) -> None:
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

            requests = list[BaseRequestData]()
            user_data = user_data or {}
            if label is not None:
                user_data.setdefault('label', label)
            for link in self._parser.find_links(parsed_content, selector=selector):
                url = link
                if not is_url_absolute(url):
                    url = convert_to_absolute_url(context.request.url, url)
                try:
                    request = BaseRequestData.from_url(url, user_data=user_data)
                except ValidationError as exc:
                    context.log.debug(
                        f'Skipping URL "{url}" due to invalid format: {exc}. '
                        'This may be caused by a malformed URL or unsupported URL scheme. '
                        'Please ensure the URL is correct and retry.'
                    )
                    continue

                requests.append(request)

            await context.add_requests(requests, **kwargs)

        return enqueue_links

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Make http request and create context enhanced by HTTP response.

        Args:
            context: The current crawling context.

        Yields:
            The original crawling context enhanced by HTTP response.
        """
        result = await self._http_client.crawl(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            statistics=self._statistics,
        )

        yield HttpCrawlingContext.from_basic_crawling_context(context=context, http_response=result.http_response)

    async def _handle_blocked_request(
        self, context: ParsedHttpCrawlingContext[TParseResult]
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        """Try to detect if the request is blocked based on the HTTP status code or the parsed response content.

        Args:
            context: The current crawling context.

        Raises:
            SessionError: If the request is considered blocked.

        Yields:
            The original crawling context if no errors are detected.
        """
        if self._retry_on_blocked:
            status_code = context.http_response.status_code
            if self._is_session_blocked_status_code(context.session, status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')
            if blocked_info := self._parser.is_blocked(context.parsed_content):
                raise SessionError(blocked_info.reason)
        yield context

    def pre_navigation_hook(self, hook: Callable[[BasicCrawlingContext], Awaitable[None]]) -> None:
        """Register a hook to be called before each navigation.

        Args:
            hook: A coroutine function to be called before each navigation.
        """
        self._pre_navigation_hooks.append(hook)
