import asyncio

from crawlee.configuration import Configuration
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # highlight-next-line
    config = Configuration(purge_on_start=False)
    crawler = HttpCrawler(configuration=config)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
