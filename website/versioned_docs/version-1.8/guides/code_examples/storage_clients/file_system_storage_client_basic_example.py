from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import FileSystemStorageClient

# Create a new instance of storage client.
storage_client = FileSystemStorageClient()

# And pass it to the crawler.
crawler = ParselCrawler(storage_client=storage_client)
