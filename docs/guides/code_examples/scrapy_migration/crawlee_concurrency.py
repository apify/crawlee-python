import asyncio

from crawlee import ConcurrencySettings
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # `ConcurrencySettings` replaces Scrapy's `CONCURRENT_REQUESTS` and
    # `DOWNLOAD_DELAY`.
    concurrency_settings = ConcurrencySettings(
        # Start with this many parallel tasks.
        desired_concurrency=5,
        # Never run more than this many in parallel.
        max_concurrency=20,
        # Cap total throughput across the whole pool.
        max_tasks_per_minute=120,
    )

    crawler = ParselCrawler(
        concurrency_settings=concurrency_settings,
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')
        await context.enqueue_links(selector='li.next a')

    await crawler.run(['https://quotes.toscrape.com/'])


if __name__ == '__main__':
    asyncio.run(main())
