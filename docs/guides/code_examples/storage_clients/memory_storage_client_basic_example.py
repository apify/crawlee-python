from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient

# Create memory storage client.
storage_client = MemoryStorageClient()

# Or pass it directly to the crawler.
crawler = ParselCrawler(storage_client=storage_client)
