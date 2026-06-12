import asyncio

from crawlee import service_locator
from crawlee.storage_clients import MemoryStorageClient


async def main() -> None:
    storage_client = MemoryStorageClient()

    # Register storage client via service locator.
    service_locator.set_storage_client(storage_client)


if __name__ == '__main__':
    asyncio.run(main())
