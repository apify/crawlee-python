import asyncio

from crawlee.storage_clients import MemoryStorageClient


async def main() -> None:
    storage_client = MemoryStorageClient.from_config()
    # highlight-next-line
    await storage_client.purge_on_start()


if __name__ == '__main__':
    asyncio.run(main())
