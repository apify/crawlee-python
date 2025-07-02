from __future__ import annotations

import asyncio
import logging
from abc import ABC
from typing import TYPE_CHECKING, Any, Generic

from more_itertools import partition
from pydantic import ValidationError
from typing_extensions import TypeVar

from crawlee._request import Request, RequestOptions
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import to_absolute_url_iterator
from crawlee.crawlers._basic import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.statistics import StatisticsState

from ._http_crawling_context import HttpCrawlingContext, ParsedHttpCrawlingContext, TParseResult, TSelectResult

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator

    from typing_extensions import Unpack

    from crawlee import RequestTransformAction
    from crawlee._types import BasicCrawlingContext, EnqueueLinksKwargs, ExtractLinksFunction

    from ._abstract_http_parser import AbstractHttpParser

TCrawlingContext = TypeVar('TCrawlingContext', bound=ParsedHttpCrawlingContext)
TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)


@docs_group('Abstract classes')
class AbstractHttpCrawler(
    Generic[TCrawlingContext, TParseResult, TSelectResult], BasicCrawler[TCrawlingContext, StatisticsState], ABC
):
    """A web crawler for performing HTTP requests.

    The `AbstractHttpCrawler` builds on top of the `BasicCrawler`, inheriting all its features. Additionally,
    it implements HTTP communication using HTTP clients. The class allows integration with any HTTP client
    that implements the `HttpClient` interface, provided as an input parameter to the constructor.

    `AbstractHttpCrawler` is a generic class intended to be used with a specific parser for parsing HTTP responses
    and the expected type of `TCrawlingContext` available to the user function. Examples of specific versions include
    `BeautifulSoupCrawler`, `ParselCrawler`, and `HttpCrawler`.

    HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. For websites that
    require client-side JavaScript execution, consider using a browser-based crawler like the `PlaywrightCrawler`.
    """

    def __init__(
        self,
        *,
        parser: AbstractHttpParser[TParseResult, TSelectResult],
        **kwargs: Unpack[BasicCrawlerOptions[TCrawlingContext, StatisticsState]],
    ) -> None:
        self._parser = parser
        self._pre_navigation_hooks: list[Callable[[BasicCrawlingContext], Awaitable[None]]] = []

        if '_context_pipeline' not in kwargs:
            raise ValueError(
                'Please pass in a `_context_pipeline`. You should use the '
                'AbstractHttpCrawler._create_static_content_crawler_pipeline() method to initialize it.'
            )

        kwargs.setdefault('_logger', logging.getLogger(self.__class__.__name__))
        super().__init__(**kwargs)

    @classmethod
    def create_parsed_http_crawler_class(
        cls,
        static_parser: AbstractHttpParser[TParseResult, TSelectResult],
    ) -> type[AbstractHttpCrawler[ParsedHttpCrawlingContext[TParseResult], TParseResult, TSelectResult]]:
        """Create a specific version of `AbstractHttpCrawler` class.

        This is a convenience factory method for creating a specific `AbstractHttpCrawler` subclass.
        While `AbstractHttpCrawler` allows its two generic parameters to be independent,
        this method simplifies cases where `TParseResult` is used for both generic parameters.
        """

        class _ParsedHttpCrawler(
            AbstractHttpCrawler[ParsedHttpCrawlingContext[TParseResult], TParseResult, TSelectResult]
        ):
            def __init__(
                self,
                parser: AbstractHttpParser[TParseResult, TSelectResult] = static_parser,
                **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[TParseResult]]],
            ) -> None:
                kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline()
                super().__init__(
                    parser=parser,
                    **kwargs,
                )

        return _ParsedHttpCrawler

    def _create_static_content_crawler_pipeline(self) -> ContextPipeline[ParsedHttpCrawlingContext[TParseResult]]:
        """Create static content crawler context pipeline with expected pipeline steps."""
        return (
            ContextPipeline()
            .compose(self._execute_pre_navigation_hooks)
            .compose(self._make_http_request)
            .compose(self._handle_status_code_response)
            .compose(self._parse_http_response)
            .compose(self._handle_blocked_request_by_content)
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
        extract_links = self._create_extract_links_function(context, parsed_content)
        yield ParsedHttpCrawlingContext.from_http_crawling_context(
            context=context,
            parsed_content=parsed_content,
            enqueue_links=self._create_enqueue_links_function(context, extract_links),
            extract_links=extract_links,
        )

    def _create_extract_links_function(
        self, context: HttpCrawlingContext, parsed_content: TParseResult
    ) -> ExtractLinksFunction:
        """Create a callback function for extracting links from parsed content.

        Args:
            context: The current crawling context.
            parsed_content: The parsed http response.

        Returns:
            Awaitable that is used for extracting links from parsed content.
        """

        async def extract_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict[str, Any] | None = None,
            transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction]
            | None = None,
            **kwargs: Unpack[EnqueueLinksKwargs],
        ) -> list[Request]:
            requests = list[Request]()

            base_user_data = user_data or {}

            robots_txt_file = await self._get_robots_txt_file_for_url(context.request.url)

            kwargs.setdefault('strategy', 'same-hostname')

            links_iterator: Iterator[str] = iter(self._parser.find_links(parsed_content, selector=selector))
            links_iterator = to_absolute_url_iterator(context.request.loaded_url or context.request.url, links_iterator)

            if robots_txt_file:
                skipped, links_iterator = partition(lambda url: robots_txt_file.is_allowed(url), links_iterator)
            else:
                skipped = iter([])

            for url in self._enqueue_links_filter_iterator(links_iterator, context.request.url, **kwargs):
                request_options = RequestOptions(url=url, user_data={**base_user_data}, label=label)

                if transform_request_function:
                    transform_request_options = transform_request_function(request_options)
                    if transform_request_options == 'skip':
                        continue
                    if transform_request_options != 'unchanged':
                        request_options = transform_request_options

                try:
                    request = Request.from_url(**request_options)
                except ValidationError as exc:
                    context.log.debug(
                        f'Skipping URL "{url}" due to invalid format: {exc}. '
                        'This may be caused by a malformed URL or unsupported URL scheme. '
                        'Please ensure the URL is correct and retry.'
                    )
                    continue

                requests.append(request)

            skipped_tasks = [
                asyncio.create_task(self._handle_skipped_request(request, 'robots_txt')) for request in skipped
            ]
            await asyncio.gather(*skipped_tasks)

            return requests

        return extract_links

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

    async def _handle_status_code_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Validate the HTTP status code and raise appropriate exceptions if needed.

        Args:
            context: The current crawling context containing the HTTP response.

        Raises:
            SessionError: If the status code indicates the session is blocked.
            HttpStatusCodeError: If the status code represents a server error or is explicitly configured as an error.
            HttpClientStatusCodeError: If the status code represents a client error.

        Yields:
            The original crawling context if no errors are detected.
        """
        status_code = context.http_response.status_code
        if self._retry_on_blocked:
            self._raise_for_session_blocked_status_code(context.session, status_code)
        self._raise_for_error_status_code(status_code)
        yield context

    async def _handle_blocked_request_by_content(
        self, context: ParsedHttpCrawlingContext[TParseResult]
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        """Try to detect if the request is blocked based on the parsed response content.

        Args:
            context: The current crawling context.

        Raises:
            SessionError: If the request is considered blocked.

        Yields:
            The original crawling context if no blocking is detected.
        """
        if self._retry_on_blocked and (blocked_info := self._parser.is_blocked(context.parsed_content)):
            raise SessionError(blocked_info.reason)
        yield context

    def pre_navigation_hook(self, hook: Callable[[BasicCrawlingContext], Awaitable[None]]) -> None:
        """Register a hook to be called before each navigation.

        Args:
            hook: A coroutine function to be called before each navigation.
        """
        self._pre_navigation_hooks.append(hook)
