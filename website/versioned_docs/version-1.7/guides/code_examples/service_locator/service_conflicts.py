import asyncio

from crawlee import service_locator
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient


async def main() -> None:
    # Register the storage client via service locator.
    memory_storage_client = MemoryStorageClient()
    service_locator.set_storage_client(memory_storage_client)

    # Retrieve the storage client.
    current_storage_client = service_locator.get_storage_client()

    # Try to set a different storage client, which will raise ServiceConflictError
    # if storage client was already retrieved.
    file_system_storage_client = FileSystemStorageClient()
    service_locator.set_storage_client(file_system_storage_client)


if __name__ == '__main__':
    asyncio.run(main())
