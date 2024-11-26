from __future__ import annotations

from typing import TYPE_CHECKING

from parsel import Selector

from crawlee._utils.docs import docs_group
from crawlee.http_crawler import HttpCrawlerGeneric
from crawlee.parsel_crawler._parsel_parser import ParselParser

if TYPE_CHECKING:
    from collections.abc import Iterable

    from typing_extensions import Unpack

    from crawlee.basic_crawler import BasicCrawlerOptions
    from crawlee.http_crawler import ParsedHttpCrawlingContext


@docs_group('Classes')
class ParselCrawler(HttpCrawlerGeneric[Selector]):
    """A web crawler for performing HTTP requests and parsing HTML/XML content.

    The `ParselCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features.
    It specifies its own parser ParselParser which is used to parse HttpResponse.

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.parsel_crawler import ParselCrawler, ParselCrawlingContext

    crawler = ParselCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.selector.css('title').get(),
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
        **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[Selector]]],
    ) -> None:
        """A default constructor.

        Args:
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors, triggering
                automatic retries when encountered.
            ignore_http_error_status_codes: HTTP status codes typically considered errors but to be treated
                as successful responses.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        super().__init__(
            parser=ParselParser(),
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
            **kwargs,
        )
