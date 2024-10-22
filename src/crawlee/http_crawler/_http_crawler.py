from __future__ import annotations

import logging
from typing import TYPE_CHECKING, AsyncGenerator, Iterable

from typing_extensions import Unpack

from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient
from crawlee.http_crawler._http_crawling_context import HttpCrawlingContext

if TYPE_CHECKING:
    from crawlee._types import BasicCrawlingContext


class HttpCrawler(BasicCrawler[HttpCrawlingContext]):
    """A crawler that fetches the request URL using `httpx`.

    The `HttpCrawler` class extends `BasicCrawler` to perform web crawling tasks that involve HTTP requests.
    It uses the `httpx` library for handling HTTP-based operations, supporting configurable error handling
    and session management. The crawler can manage additional error status codes to trigger retries
    and exclude specific codes that are generally treated as errors.

    Usage:
        ```python
        from crawlee.http_crawler import HttpCrawler

        # Instantiate and configure the HttpCrawler
        crawler = HttpCrawler(
            additional_http_error_status_codes=[500, 502],
            ignore_http_error_status_codes=[404],
            max_request_retries=3,
            request_timeout_secs=30,
        )

        # Run the crawler to start fetching URLs
        await crawler.run()
        ```
    """

    def __init__(
        self,
        *,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[HttpCrawlingContext]],
    ) -> None:
        """Initialize the HttpCrawler.

        Args:
            additional_http_error_status_codes: HTTP status codes that should be considered errors (and trigger a retry)

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but we want to treat
                them as successful

            kwargs: Arguments to be forwarded to the underlying BasicCrawler
        """
        kwargs['_context_pipeline'] = (
            ContextPipeline().compose(self._make_http_request).compose(self._handle_blocked_request)
        )

        kwargs.setdefault(
            'http_client',
            HttpxHttpClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
        )

        kwargs.setdefault('_logger', logging.getLogger(__name__))

        super().__init__(**kwargs)

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Executes an HTTP request using the `httpx` client with the provided context parameters.

        Args:
            context: The crawling context containing request, session,
                and other relevant parameters for the HTTP request.

        Yields:
            The context object, updated with the HTTP response details.
        """
        result = await self._http_client.crawl(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            statistics=self._statistics,
        )

        yield HttpCrawlingContext(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            log=context.log,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(self, context: HttpCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Handles blocked requests by checking the HTTP status code and managing session behavior.

        If a blocked status code is detected and the retry option is enabled,
            the session is flagged as blocked to trigger a retry mechanism.

        Args:
            context: The crawling context containing the HTTP response and session information.

        Yields:
            The same context if no errors are detected, otherwise raises a `SessionError` to indicate a blocked session.
        """
        if self._retry_on_blocked:
            status_code = context.http_response.status_code

            if context.session and context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')

        yield context
