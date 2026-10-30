from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import MemoryStorageBackend

# Create a new instance of storage backend.
storage_backend = MemoryStorageBackend()

# And pass it to the crawler.
crawler = ParselCrawler(storage_backend=storage_backend)
