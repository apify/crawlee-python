from __future__ import annotations

from typing import TYPE_CHECKING

from parsel import Selector

from crawlee._utils.docs import docs_group
from crawlee.crawlers import AbstractHttpCrawler, BasicCrawlerOptions

from ._parsel_crawling_context import ParselCrawlingContext
from ._parsel_parser import ParselParser

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from typing_extensions import Unpack

    from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext


@docs_group('Classes')
class ParselCrawler(AbstractHttpCrawler[ParselCrawlingContext, Selector, Selector]):
    """A web crawler for performing HTTP requests and parsing HTML/XML content.

    The `ParselCrawler` builds on top of the `AbstractHttpCrawler`, which means it inherits all of its features.
    It specifies its own parser `ParselParser` which is used to parse `HttpResponse`.
    `ParselParser` uses following library for parsing: https://pypi.org/project/parsel/

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.crawlers import ParselCrawler, ParselCrawlingContext

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
        **kwargs: Unpack[BasicCrawlerOptions[ParselCrawlingContext]],
    ) -> None:
        """Initialize a new instance.

        Args:
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """

        async def final_step(
            context: ParsedHttpCrawlingContext[Selector],
        ) -> AsyncGenerator[ParselCrawlingContext, None]:
            """Enhance `ParsedHttpCrawlingContext[Selector]` with a `selector` property."""
            yield ParselCrawlingContext.from_parsed_http_crawling_context(context)

        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline().compose(final_step)
        super().__init__(
            parser=ParselParser(),
            **kwargs,
        )
