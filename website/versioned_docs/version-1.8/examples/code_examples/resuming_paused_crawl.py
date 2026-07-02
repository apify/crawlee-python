import asyncio

from crawlee import ConcurrencySettings, service_locator
from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)

# Disable clearing the `RequestQueue`, `KeyValueStore` and `Dataset` on each run.
# This makes the scraper continue from where it left off in the previous run.
# The recommended way to achieve this behavior is setting the environment variable
# `CRAWLEE_PURGE_ON_START=0`
configuration = service_locator.get_configuration()
configuration.purge_on_start = False


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Let's slow down the crawler for a demonstration
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=20)
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # List of links for crawl
    requests = [
        'https://crawlee.dev',
        'https://crawlee.dev/python/docs',
        'https://crawlee.dev/python/docs/examples',
        'https://crawlee.dev/python/docs/guides',
        'https://crawlee.dev/python/docs/quick-start',
    ]

    await crawler.run(requests)


if __name__ == '__main__':
    asyncio.run(main())
