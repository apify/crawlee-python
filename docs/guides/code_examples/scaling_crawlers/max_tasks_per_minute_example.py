import asyncio

from crawlee import ConcurrencySettings
from crawlee.crawlers import BeautifulSoupCrawler


async def main() -> None:
    concurrency_settings = ConcurrencySettings(
        # Set the maximum number of concurrent requests the crawler can run to 100.
        max_concurrency=100,
        # Limit the total number of requests to 10 per minute to avoid overwhelming
        # the target website.
        max_tasks_per_minute=10,
    )

    crawler = BeautifulSoupCrawler(
        # Apply the defined concurrency settings to the crawler.
        concurrency_settings=concurrency_settings,
    )


if __name__ == '__main__':
    asyncio.run(main())
