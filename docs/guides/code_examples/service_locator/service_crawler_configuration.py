import asyncio
from datetime import timedelta

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler


async def main() -> None:
    configuration = Configuration(
        log_level='DEBUG',
        headless=False,
        persist_state_interval=timedelta(seconds=30),
    )

    # Register configuration via crawler.
    crawler = ParselCrawler(
        configuration=configuration,
    )


if __name__ == '__main__':
    asyncio.run(main())
