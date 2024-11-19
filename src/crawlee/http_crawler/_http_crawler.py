from __future__ import annotations

import logging
from typing import TYPE_CHECKING, AsyncGenerator, Iterable

from crawlee._utils.docs import docs_group
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient
from crawlee.http_crawler._http_crawling_context import HttpCrawlingContext

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from crawlee._types import BasicCrawlingContext


@docs_group('Classes')
class HttpCrawler(BasicCrawler[HttpCrawlingContext]):
    """A web crawler for performing HTTP requests.

    The `HttpCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features. On top
    of that it implements the HTTP communication using the HTTP clients. The class allows integration with
    any HTTP client that implements the `BaseHttpClient` interface. The HTTP client is provided to the crawler
    as an input parameter to the constructor.

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using a browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext

    crawler = HttpCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'response': context.http_response.read().decode()[:100],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    await crawler.run(['https://crawlee.dev/'])
    ```
    """

    def __init__(
        self,
        *,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[HttpCrawlingContext]],
    ) -> None:
        """A default constructor.

        Args:
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors, triggering
                automatic retries when encountered.
            ignore_http_error_status_codes: HTTP status codes typically considered errors but to be treated
                as successful responses.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
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
        """Executes an HTTP request using a configured HTTP client.

        Args:
            context: The crawling context from the `BasicCrawler`.

        Yields:
            The enhanced crawling context with the HTTP response.
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
            get_key_value_store=context.get_key_value_store,
            log=context.log,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(self, context: HttpCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Try to detect if the request is blocked based on the HTTP status code.

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

        yield context
