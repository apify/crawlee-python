import asyncio

from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import MemoryStorageBackend


async def main() -> None:
    storage_backend = MemoryStorageBackend()

    # Register storage backend via crawler.
    crawler = ParselCrawler(
        storage_backend=storage_backend,
    )


if __name__ == '__main__':
    asyncio.run(main())
