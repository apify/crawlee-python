from __future__ import annotations

import asyncio
from collections.abc import Sequence


from crawlee import Request
from crawlee._types import BasicCrawlingContext, RequestHandlerRunResult

from crawlee.crawlers import BeautifulSoupCrawler, BasicCrawler, BeautifulSoupCrawlingContext, PlaywrightCrawler, \
    PlaywrightCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import \
    AdaptivePlaywrightCrawlingContext
from crawlee.statistics import FinalStatistics


class AdaptivePlaywrightCrawler(BasicCrawler[AdaptivePlaywrightCrawlingContext]):

    """
    def __init__(self) -> None:
        context_pipeline = ContextPipeline().compose(self._open_page).compose(self._navigate).compose(
            self._handle_blocked_request)
        super().__init__(parser=BeautifulSoupParser(), _context_pipeline=context_pipeline)
        self._context_pipeline = ContextPipeline().compose(self._open_page).compose(self._navigate)
    """
    def __init__(self, max_requests_per_crawl: int) -> None:
        self.beautifulsoup_crawler = BeautifulSoupCrawler()
        self.playwright_crawler = PlaywrightCrawler()

        @self.beautifulsoup_crawler.router.default_handler
        async def request_handler_beautiful_soup(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing with BS: {context.request.url} ...')
            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_beautifulsoup_crawling_context(context)
            await self.router(adaptive_crawling_context)

        @self.playwright_crawler.router.default_handler
        async def request_handler_playwright(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing with PW: {context.request.url} ...')
            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(context)
            await self.router(adaptive_crawling_context)

        super().__init__(max_requests_per_crawl=max_requests_per_crawl)

    async def run(
        self,
        requests: Sequence[str | Request] | None = None,
        *,
        purge_request_queue: bool = True,
    ) -> FinalStatistics:
        async with (self.beautifulsoup_crawler.statistics, self.playwright_crawler.statistics,
                    self.playwright_crawler._additional_context_managers[0]): # TODO: Create something more robust that does not leak implementation so much
            top_crawler_statistics = await super().run(requests=requests, purge_request_queue=purge_request_queue)
        return top_crawler_statistics

    # Can't use override as mypy does not like it for double underscore pribvate method.
    async def _BasicCrawler__run_request_handler(self, context: BasicCrawlingContext) -> None:

        result = RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store)

        await self.beautifulsoup_crawler._crawl_one(context = context,
                                                    request_handler_timeout=self._request_handler_timeout,
                                                    result= result)
        await self.playwright_crawler._crawl_one(context=context,
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



