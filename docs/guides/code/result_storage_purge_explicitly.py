import asyncio
from crawlee.memory_storage_client import MemoryStorageClient

async def main() -> None:
    storage_client = MemoryStorageClient()
    await storage_client.purge_on_start()

if __name__ == '__main__':
    asyncio.run(main())
