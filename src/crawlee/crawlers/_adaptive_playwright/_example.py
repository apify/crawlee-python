import asyncio
import logging
from logging import getLogger

from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptiveContextError,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    # remove in review. Move this to documentation examples instead.
    top_logger = getLogger(__name__)
    top_logger.setLevel(logging.DEBUG)
    i = 0

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=10, _logger=top_logger, playwright_crawler_specific_kwargs={'headless': False}
    )
    """

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        max_requests_per_crawl=10, _logger=top_logger, playwright_crawler_specific_kwargs={'headless': False}
    )
    """

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal i
        i = i + 1
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')
        await context.enqueue_links()
        await context.push_data({'Top crwaler Url': context.request.url})
        await context.use_state({'bla': i})

    @crawler.pre_navigation_hook
    async def hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        try:
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            context.log.info(f'PW pre navigation hook for: {context.request.url} ...')
        except AdaptiveContextError:
            context.log.info(f'BS pre navigation hook for: {context.request.url} ...')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://warehouse-theme-metal.myshopify.com/'])


if __name__ == '__main__':
    asyncio.run(main())
