import asyncio

from crawlee import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import Dataset


async def main() -> None:
    # Create custom storage backend, MemoryStorageBackend for example.
    storage_backend = MemoryStorageBackend()

    # Register it globally via the service locator.
    service_locator.set_storage_backend(storage_backend)

    # Or pass it directly to the crawler, it will be registered globally
    # to the service locator under the hood.
    crawler = ParselCrawler(storage_backend=storage_backend)

    # Or just provide it when opening a storage (e.g. dataset), it will be used
    # for this storage only, not globally.
    dataset = await Dataset.open(
        name='my-dataset',
        storage_backend=storage_backend,
    )


if __name__ == '__main__':
    asyncio.run(main())
