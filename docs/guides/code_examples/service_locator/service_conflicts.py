import asyncio

from crawlee import service_locator
from crawlee.storage_backends import FileSystemStorageBackend, MemoryStorageBackend


async def main() -> None:
    # Register the storage backend via service locator.
    memory_storage_backend = MemoryStorageBackend()
    service_locator.set_storage_backend(memory_storage_backend)

    # Retrieve the storage backend.
    current_storage_backend = service_locator.get_storage_backend()

    # Try to set a different storage backend, which will raise ServiceConflictError
    # if storage backend was already retrieved.
    file_system_storage_backend = FileSystemStorageBackend()
    service_locator.set_storage_backend(file_system_storage_backend)


if __name__ == '__main__':
    asyncio.run(main())
