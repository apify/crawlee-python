from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Iterable, Literal

import httpx

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
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
        context_pipeline = ContextPipeline().compose(self._make_http_request).compose(self._parse_http_response)

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
        result = await self._http_client.crawl(context.request)

        yield HttpCrawlingContext(
            request=context.request, send_request=context.send_request, http_response=result.http_response
        )

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        from bs4 import BeautifulSoup

        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))
        yield BeautifulSoupCrawlingContext(
            request=context.request, send_request=context.send_request, http_response=context.http_response, soup=soup
        )
