from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Iterable, Literal

import httpx

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.basic_crawler.errors import SessionError
from crawlee.beautifulsoup_crawler.types import BeautifulSoupCrawlingContext
from crawlee.http_clients.httpx_client import HttpxClient
from crawlee.http_crawler.types import HttpCrawlingContext

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.autoscaling.autoscaled_pool import ConcurrencySettings
    from crawlee.basic_crawler.types import BasicCrawlingContext
    from crawlee.configuration import Configuration
    from crawlee.storages.request_provider import RequestProvider


class BeautifulSoupCrawler(BasicCrawler[BeautifulSoupCrawlingContext]):
    """A crawler that fetches the request URL using `httpx` and parses the result with `BeautifulSoup`."""

    def __init__(
        self,
        *,
        parser: Literal['html.parser', 'lxml', 'xml', 'html5lib'] = 'lxml',
        request_provider: RequestProvider,
        router: Callable[[BeautifulSoupCrawlingContext], Awaitable[None]] | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta | None = None,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        """Initialize the BeautifulSoupCrawler.

        Args:
            parser: The type of parser that should be used by BeautifulSoup

            request_provider: Provides requests to be processed

            router: A callable to which request handling is delegated

            concurrency_settings: Allows fine-tuning concurrency levels

            configuration: Crawler configuration

            request_handler_timeout: How long is a single request handler allowed to run

            additional_http_error_status_codes: HTTP status codes that should be considered errors (and trigger a retry)

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but we want to treat
                them as successful
        """
        context_pipeline = (
            ContextPipeline()
            .compose(self._make_http_request)
            .compose(self._parse_http_response)
            .compose(self._handle_blocked_request)
        )

        self._client = httpx.AsyncClient()
        self._parser = parser

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        super().__init__(
            request_provider=request_provider,
            router=router,
            concurrency_settings=concurrency_settings,
            configuration=configuration,
            http_client=HttpxClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
            _context_pipeline=context_pipeline,
            **basic_crawler_kwargs,  # type: ignore
        )

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await self._http_client.crawl(context.request, context.session)

        yield HttpCrawlingContext(
            request=context.request,
            session=context.session,
            send_request=context.send_request,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(
        self, crawling_context: BeautifulSoupCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        if self._retry_on_blocked:
            status_code = crawling_context.http_response.status_code

            if crawling_context.session and crawling_context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')

            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if crawling_context.soup.find(selector) is not None
            ]

            if matched_selectors:
                raise SessionError(
                    'Assuming the session is blocked - '
                    f"HTTP response matched the following selectors: {'; '.join(matched_selectors)}"
                )

        yield crawling_context

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        from bs4 import BeautifulSoup

        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))
        yield BeautifulSoupCrawlingContext(
            request=context.request,
            session=context.session,
            send_request=context.send_request,
            http_response=context.http_response,
            soup=soup,
        )
