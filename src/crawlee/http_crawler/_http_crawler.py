from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.static_content_crawler import ParsedHttpCrawlingContext, StaticContentCrawler

from ._http_parser import NoParser

if TYPE_CHECKING:
    from collections.abc import Iterable

    from typing_extensions import Unpack

    from crawlee.basic_crawler import BasicCrawlerOptions


class HttpCrawler(StaticContentCrawler[ParsedHttpCrawlingContext[bytes], bytes]):
    """Specific version of generic StaticContentCrawler.

    It uses a dummy parser that just returns the HTTP response body as is.
    It is recommended to rather use `BeautifulSoupCrawler` or `ParselCrawler` or to write your own subclass of `StaticContentCrawler`.

    ### Usage

    ```python
    from crawlee.http_crawler import StaticContentCrawler, HttpCrawlingContext

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
        **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[bytes]]],
    ) -> None:
        """A default constructor.

        Args:
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors, triggering
                automatic retries when encountered.
            ignore_http_error_status_codes: HTTP status codes typically considered errors but to be treated
                as successful responses.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline()
        super().__init__(
            parser=NoParser(),
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
            **kwargs,
        )
