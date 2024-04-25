from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Iterable

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.basic_crawler.errors import SessionError
from crawlee.http_clients.httpx_client import HttpxClient
from crawlee.http_crawler.types import HttpCrawlingContext

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.autoscaling.autoscaled_pool import ConcurrencySettings
    from crawlee.basic_crawler.types import BasicCrawlingContext
    from crawlee.configuration import Configuration
    from crawlee.storages.request_provider import RequestProvider


class HttpCrawler(BasicCrawler[HttpCrawlingContext]):
    """A crawler that fetches the request URL using `httpx`."""

    def __init__(
        self,
        *,
        request_provider: RequestProvider,
        router: Callable[[HttpCrawlingContext], Awaitable[None]] | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta | None = None,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        """Initialize the HttpCrawler.

        Args:
            request_provider: Provides requests to be processed

            router: A callable to which request handling is delegated

            concurrency_settings: Allows fine-tuning concurrency levels

            configuration: Crawler configuration

            request_handler_timeout: How long is a single request handler allowed to run

            additional_http_error_status_codes: HTTP status codes that should be considered errors (and trigger a retry)

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but we want to treat
                them as successful
        """
        context_pipeline = ContextPipeline().compose(self._make_http_request).compose(self._handle_blocked_request)

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        super().__init__(
            router=router,
            _context_pipeline=context_pipeline,
            request_provider=request_provider,
            concurrency_settings=concurrency_settings,
            configuration=configuration,
            http_client=HttpxClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
            **basic_crawler_kwargs,  # type: ignore
        )

    async def _make_http_request(
        self, crawling_context: BasicCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await self._http_client.crawl(crawling_context.request)

        yield HttpCrawlingContext(
            request=crawling_context.request,
            session=crawling_context.session,
            send_request=crawling_context.send_request,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(
        self, crawling_context: HttpCrawlingContext
    ) -> AsyncGenerator[HttpCrawlingContext, None]:
        if self._retry_on_blocked:
            status_code = crawling_context.http_response.status_code

            if crawling_context.session and crawling_context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')

        yield crawling_context
