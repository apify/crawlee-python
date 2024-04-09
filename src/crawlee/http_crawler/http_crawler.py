from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable

import httpx

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.http_crawler.types import HttpCrawlingContext

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.autoscaling.autoscaled_pool import ConcurrencySettings
    from crawlee.basic_crawler.types import BasicCrawlingContext
    from crawlee.config import Config
    from crawlee.storages.request_provider import RequestProvider


class HttpCrawler(BasicCrawler[HttpCrawlingContext]):
    """A crawler that fetches the request URL using `httpx`."""

    def __init__(
        self,
        *,
        request_provider: RequestProvider,
        router: Callable[[HttpCrawlingContext], Awaitable[None]] | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        configuration: Config | None = None,
        request_handler_timeout: timedelta | None = None,
    ) -> None:
        """Initialize the HttpCrawler.

        Args:
            request_provider: Provides requests to be processed
            router: A callable to which request handling is delegated
            concurrency_settings: Allows fine-tuning concurrency levels
            configuration: Crawler configuration
            request_handler_timeout: How long is a single request handler allowed to run
        """
        context_pipeline = ContextPipeline().compose(self._make_http_request)
        self._client = httpx.AsyncClient()

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        super().__init__(
            router=router,
            _context_pipeline=context_pipeline,
            request_provider=request_provider,
            concurrency_settings=concurrency_settings,
            configuration=configuration,
            **basic_crawler_kwargs,  # type: ignore
        )

    async def _make_http_request(
        self, crawling_context: BasicCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        response = await self._client.request(crawling_context.request.method, crawling_context.request.url)
        response.raise_for_status()

        yield HttpCrawlingContext(request=crawling_context.request, http_response=response)
