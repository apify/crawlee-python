import asyncio

from crawlee import ConcurrencySettings
from crawlee.crawlers import BeautifulSoupCrawler


async def main() -> None:
    concurrency_settings = ConcurrencySettings(
        # Start with 8 concurrent tasks, as long as resources are available.
        desired_concurrency=8,
        # Maintain a minimum of 5 concurrent tasks to ensure steady crawling.
        min_concurrency=5,
        # Limit the maximum number of concurrent tasks to 10 to prevent
        # overloading the system.
        max_concurrency=10,
    )

    crawler = BeautifulSoupCrawler(
        # Use the configured concurrency settings for the crawler.
        concurrency_settings=concurrency_settings,
    )

    # ...


if __name__ == '__main__':
    asyncio.run(main())
