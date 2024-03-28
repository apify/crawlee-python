from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable

import httpx

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.http_crawler.types import HttpCrawlingContext

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.basic_crawler.types import BasicCrawlingContext
    from crawlee.config import Configuration
    from crawlee.storages.request_provider import RequestProvider


class HttpCrawler(BasicCrawler[HttpCrawlingContext]):
    """A crawler that fetches the request URL using `httpx`."""

    def __init__(
        self,
        *,
        router: Callable[[HttpCrawlingContext], Awaitable[None]] | None = None,
        request_provider: RequestProvider,
        min_concurrency: int | None = None,
        max_concurrency: int | None = None,
        max_requests_per_minute: int | None = None,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta | None = None,
    ) -> None:
        context_pipeline = ContextPipeline().compose(self._make_http_request)
        self._client = httpx.AsyncClient()

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        super().__init__(
            router=router,
            _context_pipeline=context_pipeline,
            request_provider=request_provider,
            min_concurrency=min_concurrency,
            max_concurrency=max_concurrency,
            max_requests_per_minute=max_requests_per_minute,
            configuration=configuration,
            **basic_crawler_kwargs,
        )

    async def _make_http_request(
        self, crawling_context: BasicCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        response = await self._client.request(crawling_context.request.method, crawling_context.request.url)
        response.raise_for_status()

        yield HttpCrawlingContext(request=crawling_context.request, http_response=response)
