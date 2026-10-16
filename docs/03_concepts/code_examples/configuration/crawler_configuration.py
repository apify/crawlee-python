import asyncio

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Keep this crawler's storages in a separate directory and don't purge them on
    # start. A second run finds the request already handled and keeps the dataset.
    configuration = Configuration(
        storage_dir='./my_crawler_storage',
        purge_on_start=False,
    )

    # The configuration applies to this crawler instance only.
    crawler = ParselCrawler(
        configuration=configuration,
        max_requests_per_crawl=1,
    )

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        await context.push_data({'url': context.request.url})

    await crawler.run(['https://crawlee.dev'])

    data = await crawler.get_data()
    print(f'Dataset holds {len(data.items)} items.')


if __name__ == '__main__':
    asyncio.run(main())
