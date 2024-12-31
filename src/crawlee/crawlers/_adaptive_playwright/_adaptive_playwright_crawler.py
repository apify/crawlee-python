from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from IPython.core.completer import TypedDict

from crawlee._types import BasicCrawlingContext, RequestHandlerRunResult
from crawlee._utils.docs import docs_group
from crawlee.crawlers import (
    BasicCrawler,
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
    BeautifulSoupParserType,
    ContextPipeline,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
)
from crawlee.crawlers._abstract_http._abstract_http_crawler import _HttpCrawlerOptions
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from typing_extensions import NotRequired, Unpack

    from crawlee import Request
    from crawlee.browsers import BrowserPool
    from crawlee.browsers._types import BrowserType
    from crawlee.crawlers._basic._basic_crawler import _BasicCrawlerOptions
    from crawlee.statistics import FinalStatistics


@docs_group('Data structures')
class _BeautifulsoupCrawlerAdditionalOptions(_HttpCrawlerOptions):
    """Additional options that can be specified for BeautifulsoupCrawler."""

    parser: NotRequired[BeautifulSoupParserType]
    """Parser type used by BeautifulSoup."""


@docs_group('Data structures')
class _PlaywrightCrawlerAdditionalOptions(TypedDict):
    """Additional options that can be specified for PlaywrightCrawler."""

    browser_pool: NotRequired[BrowserPool]
    """A `BrowserPool` instance to be used for launching the browsers and getting pages."""

    browser_type: NotRequired[BrowserType]
    """The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided."""


    browser_launch_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
                This option should not be used if `browser_pool` is provided."""

    browser_new_context_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                Playwright documentation: https://playwright.dev/python/docs/api/class-browser#browser-new-context.
                This option should not be used if `browser_pool` is provided."""

    headless: NotRequired[bool]
    """Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided."""


class AdaptivePlaywrightCrawler(BasicCrawler[AdaptivePlaywrightCrawlingContext]):
    """Adaptive crawler that uses both BeautifulSoup crawler and PlaywrightCrawler."""

    def __init__(self,
                 beautifulsoup_crawler_kwargs: _BeautifulsoupCrawlerAdditionalOptions | None = None,
                 playwright_crawler_args: _PlaywrightCrawlerAdditionalOptions | None = None,
                 request_handler: Callable[[AdaptivePlaywrightCrawlingContext], Awaitable[None]] | None = None,
                 _context_pipeline: ContextPipeline[AdaptivePlaywrightCrawlingContext] | None = None,
                 **kwargs: Unpack[_BasicCrawlerOptions]) -> None:

        beautifulsoup_crawler_kwargs = beautifulsoup_crawler_kwargs or {}
        beautifulsoup_crawler_kwargs.setdefault('parser', 'lxml')
        playwright_crawler_args = playwright_crawler_args or {}

        self.beautifulsoup_crawler = BeautifulSoupCrawler(**beautifulsoup_crawler_kwargs, **kwargs)
        self.playwright_crawler = PlaywrightCrawler(**playwright_crawler_args, **kwargs)

        @self.beautifulsoup_crawler.router.default_handler
        async def request_handler_beautiful_soup(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing with BS: {context.request.url} ...')
            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_beautifulsoup_crawling_context(context)
            await self.router(adaptive_crawling_context)

        @self.playwright_crawler.router.default_handler
        async def request_handler_playwright(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing with PW: {context.request.url} ...')
            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                context=context, beautiful_soup_parser_type=beautifulsoup_crawler_kwargs['parser'])
            await self.router(adaptive_crawling_context)

        super().__init__(request_handler=request_handler, _context_pipeline=_context_pipeline, **kwargs)

    async def run(
        self,
        requests: Sequence[str | Request] | None = None,
        *,
        purge_request_queue: bool = True,
    ) -> FinalStatistics:
        async with (self.beautifulsoup_crawler.statistics, self.playwright_crawler.statistics,
                    self.playwright_crawler._additional_context_managers[0]): # TODO: Create something more robust that does not leak implementation so much
            return await super().run(requests=requests, purge_request_queue=purge_request_queue)

    # Can't use override as mypy does not like it for double underscore private method.
    async def _BasicCrawler__run_request_handler(self, context: BasicCrawlingContext) -> None: # noqa: N802

        result = RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store)

        await self.beautifulsoup_crawler.crawl_one(context = context,
                                                    request_handler_timeout=self._request_handler_timeout,
                                                    result= result)
        await self.playwright_crawler.crawl_one(context=context,
                                                request_handler_timeout=self._request_handler_timeout,
                                                result=result)
        await self.commit_result(result = result, context = context)

    async def commit_result(self, result: RequestHandlerRunResult, context: BasicCrawlingContext) -> None:
        result_tasks = []
        result_tasks.extend([
            asyncio.create_task(context.push_data(**kwargs)) for kwargs in result.push_data_calls])
        result_tasks.extend([
            asyncio.create_task(context.add_requests(**kwargs)) for kwargs in result.add_requests_calls])
        await asyncio.gather(*result_tasks)



