import asyncio
import logging

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Configure logging before creating the crawler. Crawlee then leaves the root logger
    # as it is.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    # DEBUG for the crawlers only. The rest of Crawlee stays at INFO.
    logging.getLogger('crawlee.crawlers').setLevel(logging.DEBUG)

    crawler = ParselCrawler(max_requests_per_crawl=5)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info('Processing %s ...', context.request.url)
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
