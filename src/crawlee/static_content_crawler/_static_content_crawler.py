from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Generic

from pydantic import ValidationError
from typing_extensions import TypeVar

from crawlee import EnqueueStrategy
from crawlee._request import BaseRequestData
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient
from crawlee.static_content_crawler._static_crawling_context import (
    HttpCrawlingContext,
    ParsedHttpCrawlingContext,
    TParseResult,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from typing_extensions import Unpack

    from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, EnqueueLinksKwargs
    from crawlee.static_content_crawler._static_content_parser import StaticContentParser

TCrawlingContext = TypeVar('TCrawlingContext', bound=ParsedHttpCrawlingContext)


class StaticContentCrawler(Generic[TCrawlingContext, TParseResult], BasicCrawler[TCrawlingContext]):
    """A web crawler for performing HTTP requests.

    The `StaticContentCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features. On top
    of that it implements the HTTP communication using the HTTP clients. The class allows integration with
    any HTTP client that implements the `BaseHttpClient` interface. The HTTP client is provided to the crawler
    as an input parameter to the constructor.
    StaticContentCrawler is generic class and is expected to be used together with specific parser that will be used to
    parse http response. See prepared specific version of it: BeautifulSoupCrawler or ParselCrawler for example.
    (For backwards compatibility you can use already specific version HttpCrawler, which uses dummy
    parser.)

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using a browser-based crawler like the `PlaywrightCrawler`.
    """

    def __init__(
        self,
        *,
        parser: StaticContentParser[TParseResult],
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[TCrawlingContext]],
    ) -> None:
        self._parser = parser

        kwargs.setdefault(
            'http_client',
            HttpxHttpClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
        )

        if '_context_pipeline' not in kwargs:
            raise ValueError(
                'Please pass in a `_context_pipeline`. '
                'You should use the StaticContentCrawler._build_context_pipeline() method to initialize it.'
            )

        kwargs.setdefault('_logger', logging.getLogger(__name__))
        super().__init__(**kwargs)

    def _build_context_pipeline(self) -> ContextPipeline[ParsedHttpCrawlingContext[TParseResult]]:
        return (
            ContextPipeline()
            .compose(self._make_http_request)
            .compose(self._parse_http_response)
            .compose(self._handle_blocked_request)
        )

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        parsed_content = await self._parser.parse(context.http_response)
        yield ParsedHttpCrawlingContext.from_http_crawling_context(
            context=context,
            parsed_content=parsed_content,
            enqueue_links=self._create_enqueue_links_function(context, parsed_content),
        )

    def _create_enqueue_links_function(
        self, context: HttpCrawlingContext, parsed_content: TParseResult
    ) -> EnqueueLinksFunction:
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

            # TODO: refactor to avoid private member access
            # https://github.com/apify/crawlee-python/issues/708
            if (
                context.session
                and status_code not in self._http_client._ignore_http_error_status_codes  # noqa: SLF001
                and context.session.is_blocked_status_code(status_code=status_code)
            ):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')
            if blocked_info := self._parser.is_blocked(context.parsed_content):
                raise SessionError(blocked_info.reason)
        yield context
