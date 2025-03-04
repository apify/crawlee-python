import asyncio

from crawlee.crawlers import HttpCrawler
from crawlee.storage_clients import MemoryStorageClient


async def main() -> None:
    storage_client = MemoryStorageClient.from_config()

    # Call the purge_on_start method to explicitly purge the storage.
    # highlight-next-line
    await storage_client.purge_on_start()

    # Pass the storage client to the crawler.
    crawler = HttpCrawler(storage_client=storage_client)

    # ...


if __name__ == '__main__':
    asyncio.run(main())
