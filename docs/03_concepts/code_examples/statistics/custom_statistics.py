import asyncio
from datetime import timedelta

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.statistics import Statistics


async def main() -> None:
    statistics = Statistics.with_default_state(
        # Log the current statistics every second.
        log_interval=timedelta(seconds=1),
        log_message='Crawler statistics',
        # Log values as extra fields of a single message.
        statistics_log_format='inline',
        # Persist the statistics to the default key-value store.
        persistence_enabled=True,
        persist_state_key='MY_CRAWLER_STATISTICS',
    )

    crawler = ParselCrawler(
        statistics=statistics,
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    # Read the persisted statistics back from the key-value store.
    kvs = await crawler.get_key_value_store()
    persisted = await kvs.get_value('MY_CRAWLER_STATISTICS')
    print(f'Persisted finished requests: {persisted["requestsFinished"]}')


if __name__ == '__main__':
    asyncio.run(main())
