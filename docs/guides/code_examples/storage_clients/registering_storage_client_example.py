import asyncio

from crawlee import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset


async def main() -> None:
    # Create custom storage client, MemoryStorageClient for example.
    storage_client = MemoryStorageClient()

    # Register it globally via the service locator.
    service_locator.set_storage_client(storage_client)

    # Or pass it directly to the crawler, it will be registered globally
    # to the service locator under the hood.
    crawler = ParselCrawler(storage_client=storage_client)

    # Or just provide it when opening a storage (e.g. dataset), it will be used
    # for this storage only, not globally.
    dataset = await Dataset.open(
        name='my_dataset',
        storage_client=storage_client,
    )


if __name__ == '__main__':
    asyncio.run(main())
