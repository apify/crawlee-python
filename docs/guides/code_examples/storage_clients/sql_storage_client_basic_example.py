from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import SQLStorageClient


async def main() -> None:
    # Create a new instance of storage client.
    # Use the context manager to ensure that connections are properly cleaned up.
    async with SQLStorageClient() as storage_client:
        # And pass it to the crawler.
        crawler = ParselCrawler(storage_client=storage_client)
