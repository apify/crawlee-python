from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import SqlStorageBackend


async def main() -> None:
    # Create a new instance of storage backend.
    # This will create an SQLite database file crawlee.db or created tables in your
    # database if you pass `connection_string` or `engine`
    # Use the context manager to ensure that connections are properly cleaned up.
    async with SqlStorageBackend() as storage_backend:
        # And pass it to the crawler.
        crawler = ParselCrawler(storage_backend=storage_backend)
