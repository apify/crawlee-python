from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import SqlStorageClient


async def main() -> None:
    # Create a new instance of storage client.
    # This will create an SQLite database file crawlee.db or created tables in your
    # database if you pass `connection_string` or `engine`
    # Use the context manager to ensure that connections are properly cleaned up.
    async with SqlStorageClient() as storage_client:
        # And pass it to the crawler.
        crawler = ParselCrawler(storage_client=storage_client)
