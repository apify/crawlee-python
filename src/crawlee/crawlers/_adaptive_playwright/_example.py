import asyncio

from crawlee.crawlers import PlaywrightCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import AdaptivePlaywrightCrawler
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import \
    AdaptivePlaywrightCrawlingContext


async def main() ->None:
    crawler = AdaptivePlaywrightCrawler(max_requests_per_crawl=2)

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing with Top adaptive_crawler: {context.request.url} ...')
        await context.enqueue_links()
        await context.push_data({'Top crwaler Url': context.request.url})


    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
