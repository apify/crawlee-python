from __future__ import annotations

import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Stop the crawl after 50 requests.
        max_requests_per_crawl=50,
        # Follow links at most 2 levels away from the start URL.
        max_crawl_depth=2,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(
            'Processing %s at depth %d ...',
            context.request.url,
            context.request.crawl_depth,
        )

        # Enqueue at most 10 links from each page.
        await context.enqueue_links(limit=10)

    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
