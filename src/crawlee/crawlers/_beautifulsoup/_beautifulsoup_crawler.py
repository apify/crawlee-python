from __future__ import annotations

from typing import TYPE_CHECKING

from bs4 import BeautifulSoup, Tag

from crawlee._utils.docs import docs_group
from crawlee.crawlers import AbstractHttpCrawler, BasicCrawlerOptions

from ._beautifulsoup_crawling_context import BeautifulSoupCrawlingContext
from ._beautifulsoup_parser import BeautifulSoupParser, BeautifulSoupParserType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from typing_extensions import Unpack

    from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext


@docs_group('Classes')
class BeautifulSoupCrawler(AbstractHttpCrawler[BeautifulSoupCrawlingContext, BeautifulSoup, Tag]):
    """A web crawler for performing HTTP requests and parsing HTML/XML content.

    The `BeautifulSoupCrawler` builds on top of the `AbstractHttpCrawler`, which means it inherits all of its features.
    It specifies its own parser `BeautifulSoupParser` which is used to parse `HttpResponse`.
    `BeautifulSoupParser` uses following library for parsing: https://pypi.org/project/beautifulsoup4/

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

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
        parser: BeautifulSoupParserType = 'lxml',
        **kwargs: Unpack[BasicCrawlerOptions[BeautifulSoupCrawlingContext]],
    ) -> None:
        """A default constructor.

        Args:
            parser: The type of parser that should be used by `BeautifulSoup`.
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """

        async def final_step(
            context: ParsedHttpCrawlingContext[BeautifulSoup],
        ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
            """Enhance `ParsedHttpCrawlingContext[BeautifulSoup]` with `soup` property."""
            yield BeautifulSoupCrawlingContext.from_parsed_http_crawling_context(context)

        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline().compose(final_step)

        super().__init__(
            parser=BeautifulSoupParser(parser=parser),
            **kwargs,
        )
