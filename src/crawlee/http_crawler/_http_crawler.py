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
    """A crawler that performs HTTP requests using a configurable HTTP client.

    The `HttpCrawler` class extends `BasicCrawler` to perform web crawling tasks that involve HTTP requests.
    It supports any HTTP client that implements the `BaseHttpClient` interface, allowing for configurable
    error handling, session management, and additional HTTP behaviors. The crawler can manage specific error
    status codes to trigger retries and handle exceptions, as well as exclude codes usually treated as errors.

    Usage:
        ```python
        from crawlee.http_crawler import HttpCrawler
        from crawlee.http_clients import HttpxHttpClient
        from crawlee import Request

        # Define URLs to crawl with custom metadata
        urls_to_crawl = [
            Request(
                url="https://jsonplaceholder.typicode.com/posts/1",
                uniqueKey="post_1",
                id="1"
            ),
            Request(
                url="https://jsonplaceholder.typicode.com/posts/2",
                uniqueKey="post_2",
                id="2"
            )
        ]

        async def run_crawler():
            # Create a custom HTTP client with specific error handling
            http_client = HttpxHttpClient(
                additional_http_error_status_codes=[500, 502],
                ignore_http_error_status_codes=[404],
                timeout=10
            )

            # Initialize crawler with custom configuration
            crawler = HttpCrawler(
                http_client=http_client,
                max_request_retries=3
            )
            # Start crawling with the defined URLs
            await crawler.run(urls_to_crawl)

        # Run the crawler using asyncio
        if __name__ == "__main__":
            import asyncio
            asyncio.run(run_crawler())
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
            additional_http_error_status_codes: HTTP status codes that should be considered errors
                (and trigger a retry).

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but should be treated
                as successful.

            kwargs: Additional arguments to be forwarded to the underlying `BasicCrawler`. It includes parameters
                for configuring the HTTP client, logging, and other behaviors.
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
        """Executes an HTTP request using a configured HTTP client with the provided context parameters.

        Args:
            context: The crawling context containing request, session, and other relevant parameters
                for the HTTP request.

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
