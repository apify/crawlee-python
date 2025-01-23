import asyncio

from crawlee.configuration import Configuration
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # Set the purge_on_start field to False to avoid purging the storage on start.
    # highlight-next-line
    configuration = Configuration(purge_on_start=False)

    # Pass the configuration to the crawler.
    crawler = HttpCrawler(configuration=configuration)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
