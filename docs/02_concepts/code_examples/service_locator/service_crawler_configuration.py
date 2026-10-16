import asyncio

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler


async def main() -> None:
    configuration = Configuration(
        purge_on_start=False,
    )

    # Register configuration via crawler.
    crawler = ParselCrawler(
        configuration=configuration,
    )


if __name__ == '__main__':
    asyncio.run(main())
