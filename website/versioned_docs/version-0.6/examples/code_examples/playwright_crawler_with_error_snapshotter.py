import asyncio
from random import choice

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.statistics import Statistics


async def main() -> None:
    crawler = PlaywrightCrawler(
        statistics=Statistics.with_default_state(save_error_snapshots=True)
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        # Simulate various errors to demonstrate `ErrorSnapshotter`
        # saving only the first occurrence of unique error.
        await context.enqueue_links()
        random_number = choice(range(10))
        if random_number == 1:
            raise KeyError('Some KeyError')
        if random_number == 2:
            raise ValueError('Some ValueError')
        if random_number == 3:
            raise RuntimeError('Some RuntimeError')

    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
