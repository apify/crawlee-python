from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Literal

import httpx

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.beautifulsoup_crawler.types import BeautifulSoupCrawlingContext
from crawlee.http_crawler.http_crawler import make_http_request
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
    ) -> None:
        """Initialize the BeautifulSoupCrawler."""
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
            _context_pipeline=context_pipeline,
            **basic_crawler_kwargs,  # type: ignore
        )

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await make_http_request(self._client, context.request)
        yield HttpCrawlingContext(request=context.request, http_response=result.http_response)

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        from bs4 import BeautifulSoup

        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))
        yield BeautifulSoupCrawlingContext(request=context.request, http_response=context.http_response, soup=soup)
