from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group
from crawlee.crawlers._abstract_http import AbstractHttpCrawler, ParsedHttpCrawlingContext

from ._http_parser import NoParser

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from crawlee.crawlers import BasicCrawlerOptions


@docs_group('Classes')
class HttpCrawler(AbstractHttpCrawler[ParsedHttpCrawlingContext[bytes], bytes, bytes]):
    """Specific version of generic `AbstractHttpCrawler`.

    It uses a dummy parser that simply returns the HTTP response body as-is. Use this only if you know what you are
    doing. In most cases, using an HTML parser would be more beneficial. For such scenarios, consider using
    `BeautifulSoupCrawler`, `ParselCrawler`, or writing your own subclass of `AbstractHttpCrawler`.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

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
        **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[bytes]]],
    ) -> None:
        """A default constructor.

        Args:
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """
        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline()
        super().__init__(
            parser=NoParser(),
            **kwargs,
        )
