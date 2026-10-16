import asyncio

from crawlee import Glob
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}.')

        # Enqueue links that match the 'include' glob pattern and
        # do not match the 'exclude' glob pattern.
        # highlight-next-line
        await context.enqueue_links(
            # highlight-next-line
            include=[Glob('https://someplace.com/**/cats')],
            # highlight-next-line
            exclude=[Glob('https://**/archive/**')],
            # highlight-next-line
        )

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
