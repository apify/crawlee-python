import asyncio
from datetime import timedelta

from crawlee.crawlers import ParselCrawler
from crawlee.events import LocalEventManager


async def main() -> None:
    event_manager = LocalEventManager(
        system_info_interval=timedelta(seconds=5),
    )

    # Register event manager via crawler.
    crawler = ParselCrawler(
        event_manager=event_manager,
    )


if __name__ == '__main__':
    asyncio.run(main())
