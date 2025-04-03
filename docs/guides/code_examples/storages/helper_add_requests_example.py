import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        # highlight-next-line
        await context.add_requests(['https://apify.com/'])

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
