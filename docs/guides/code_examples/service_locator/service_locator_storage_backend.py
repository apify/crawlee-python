import asyncio

from crawlee import service_locator
from crawlee.storage_backends import MemoryStorageBackend


async def main() -> None:
    storage_backend = MemoryStorageBackend()

    # Register storage backend via service locator.
    service_locator.set_storage_backend(storage_backend)


if __name__ == '__main__':
    asyncio.run(main())
