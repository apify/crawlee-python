import asyncio

from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset


async def main() -> None:
    storage_client = MemoryStorageClient()

    # Pass the storage client to the dataset (or other storage) when opening it.
    dataset = await Dataset.open(
        storage_client=storage_client,
    )


if __name__ == '__main__':
    asyncio.run(main())
