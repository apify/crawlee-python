import asyncio

from crawlee import ConcurrencySettings
from crawlee.autoscaling import ConcurrencySystem
from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
    ParselCrawler,
    ParselCrawlingContext,
)
from crawlee.storages import RequestQueue


async def main() -> None:
    concurrency_settings = ConcurrencySettings(max_concurrency=10)

    # Each crawler needs a request queue of its own, otherwise they would both
    # consume the default one and process each other's requests.
    first_queue = await RequestQueue.open(alias='first-crawler')
    second_queue = await RequestQueue.open(alias='second-crawler')

    # One budget for both crawlers, instead of one each.
    async with ConcurrencySystem(concurrency_settings) as concurrency_system:
        first_crawler = BeautifulSoupCrawler(
            concurrency_system=concurrency_system,
            request_manager=first_queue,
        )
        second_crawler = ParselCrawler(
            concurrency_system=concurrency_system,
            request_manager=second_queue,
        )

        @first_crawler.router.default_handler
        async def first_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url}...')

        @second_crawler.router.default_handler
        async def second_handler(context: ParselCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url}...')

        await asyncio.gather(
            first_crawler.run(['https://crawlee.dev/']),
            second_crawler.run(['https://crawlee.dev/python/']),
        )


if __name__ == '__main__':
    asyncio.run(main())
