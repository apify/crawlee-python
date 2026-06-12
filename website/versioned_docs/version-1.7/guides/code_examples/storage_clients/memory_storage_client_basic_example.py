from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient

# Create a new instance of storage client.
storage_client = MemoryStorageClient()

# And pass it to the crawler.
crawler = ParselCrawler(storage_client=storage_client)
