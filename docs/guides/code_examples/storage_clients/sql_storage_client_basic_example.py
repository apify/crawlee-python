from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import SqlStorageClient


async def main() -> None:
    # Create a new instance of storage client.
    # This will also create an SQLite database file crawlee.db.
    # Use the context manager to ensure that connections are properly cleaned up.
    async with SqlStorageClient() as storage_client:
        # And pass it to the crawler.
        crawler = ParselCrawler(storage_client=storage_client)
