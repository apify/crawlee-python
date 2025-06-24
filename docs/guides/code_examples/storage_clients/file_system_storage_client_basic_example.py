from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import FileSystemStorageClient

# Create file system storage client.
storage_client = FileSystemStorageClient()

# Or pass it directly to the crawler.
crawler = ParselCrawler(storage_client=storage_client)
