import asyncio

from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient


async def main() -> None:
    storage_client = MemoryStorageClient()

    # Register storage client via crawler.
    crawler = ParselCrawler(
        storage_client=storage_client,
    )


if __name__ == '__main__':
    asyncio.run(main())
