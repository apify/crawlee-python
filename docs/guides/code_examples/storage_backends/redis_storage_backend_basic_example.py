from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import RedisStorageBackend

# Create a new instance of storage backend using connection string.
# 'redis://localhost:6379' is the just placeholder, replace it with your actual
# connection string.
storage_backend = RedisStorageBackend(connection_string='redis://localhost:6379')

# And pass it to the crawler.
crawler = ParselCrawler(storage_backend=storage_backend)
