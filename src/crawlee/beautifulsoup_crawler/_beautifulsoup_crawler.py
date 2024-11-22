from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Literal

from bs4 import BeautifulSoup

from crawlee.beautifulsoup_crawler._beautifulsoup_crawling_context import BeautifulSoupCrawlingContext
from crawlee.parsers.static_content_parser import BeautifulSoupContentParser, HttpCrawler

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from crawlee.basic_crawler import BasicCrawlerOptions
    from crawlee.http_crawler import HttpCrawlingContext


BeautifulSoupParser = Literal['html.parser', 'lxml', 'xml', 'html5lib']


class BeautifulSoupCrawler(HttpCrawler[BeautifulSoup, BeautifulSoupCrawlingContext]):
    """A web crawler for performing HTTP requests and parsing HTML/XML content.

    The `BeautifulSoupCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features.
    On top of that it implements the HTTP communication using the HTTP clients and HTML/XML parsing using the
    `BeautifulSoup` library. The class allows integration with any HTTP client that implements the `BaseHttpClient`
    interface. The HTTP client is provided to the crawler as an input parameter to the constructor.

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

    crawler = BeautifulSoupCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    await crawler.run(['https://crawlee.dev/'])
    ```
    """

    def __init__(
        self,
        *,
        parser: BeautifulSoupParser = 'lxml',
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[HttpCrawlingContext]],
    ) -> None:
        """A default constructor.

        Args:
            parser: The type of parser that should be used by `BeautifulSoup`.
            additional_http_error_status_codes: Additional HTTP status codes to treat as errors, triggering
                automatic retries when encountered.
            ignore_http_error_status_codes: HTTP status codes typically considered errors but to be treated
                as successful responses.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        super().__init__(
            parser=BeautifulSoupContentParser(parser=parser),
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
            _crawling_context_type=BeautifulSoupCrawlingContext,
            **kwargs,
        )
