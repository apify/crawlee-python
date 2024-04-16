from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Awaitable, Callable, Iterable

import httpx

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.http_crawler.types import HttpCrawlingContext, HttpCrawlResult

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.autoscaling.autoscaled_pool import ConcurrencySettings
    from crawlee.basic_crawler.types import BasicCrawlingContext
    from crawlee.configuration import Configuration
    from crawlee.request import Request
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
        context_pipeline = ContextPipeline().compose(self._make_http_request)
        self._client = httpx.AsyncClient()

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        self._additional_http_error_status_codes = set(additional_http_error_status_codes)
        self._ignore_http_error_status_codes = set(ignore_http_error_status_codes)

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
        result = await make_http_request(
            self._client,
            crawling_context.request,
            additional_http_error_status_codes=self._additional_http_error_status_codes,
            ignore_http_error_status_codes=self._ignore_http_error_status_codes,
        )

        yield HttpCrawlingContext(request=crawling_context.request, http_response=result.http_response)


async def make_http_request(
    client: httpx.AsyncClient,
    request: Request,
    *,
    additional_http_error_status_codes: set[int] | None = None,
    ignore_http_error_status_codes: set[int] | None = None,
) -> HttpCrawlResult:
    """Perform a request using `httpx`."""
    response = await client.request(request.method, request.url, follow_redirects=True)

    exclude_error = (
        ignore_http_error_status_codes is not None and response.status_code in ignore_http_error_status_codes
    )
    include_error = additional_http_error_status_codes and response.status_code in additional_http_error_status_codes

    if (response.is_server_error and not exclude_error) or include_error:
        if include_error:
            raise httpx.HTTPStatusError(
                f'Status code {response.status_code} (user-configured to be an error) returned',
                request=response.request,
                response=response,
            )

        raise httpx.HTTPStatusError(
            f'Status code {response.status_code} returned', request=response.request, response=response
        )

    request.loaded_url = str(response.url)

    return HttpCrawlResult(http_response=response)
