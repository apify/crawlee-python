import asyncio

from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import Dataset


async def main() -> None:
    storage_backend = MemoryStorageBackend()

    # Pass the storage backend to the dataset (or other storage) when opening it.
    dataset = await Dataset.open(
        storage_backend=storage_backend,
    )


if __name__ == '__main__':
    asyncio.run(main())
