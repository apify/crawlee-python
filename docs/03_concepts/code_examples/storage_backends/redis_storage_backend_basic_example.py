from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import RedisStorageBackend


async def main() -> None:
    # Create a new instance of the storage backend using a connection string.
    # 'redis://localhost:6379' is just a placeholder. Replace it with your actual
    # connection string.
    # Use the context manager to ensure that connections are properly cleaned up.
    async with RedisStorageBackend(
        connection_string='redis://localhost:6379'
    ) as storage_backend:
        # And pass it to the crawler.
        crawler = ParselCrawler(storage_backend=storage_backend)

        # Run the crawler inside the block, while the storage backend is open.
        await crawler.run(['https://crawlee.dev'])
