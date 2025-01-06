import asyncio
import logging
from logging import getLogger

from crawlee._types import BasicCrawlingContext
from crawlee.crawlers import PlaywrightPreNavCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
)


async def main() ->None:
    # TODO: remove in review
    top_logger = getLogger(__name__)
    top_logger.setLevel(logging.DEBUG)
    i=0

    crawler = AdaptivePlaywrightCrawler(max_requests_per_crawl=10,
                                        _logger=top_logger,
                                        playwright_crawler_args={"headless":False})

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal i
        i = i+1
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')
        await context.enqueue_links()
        await context.push_data({'Top crwaler Url': context.request.url})
        await context.use_state({"bla":i})

    @crawler.pre_navigation_hook_bs
    async def bs_hook(context: BasicCrawlingContext) -> None:
        context.log.info(f'BS pre navigation hook for: {context.request.url} ...')

    @crawler.pre_navigation_hook_pw
    async def pw_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        context.log.info(f'PW pre navigation hook for: {context.request.url} ...')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
