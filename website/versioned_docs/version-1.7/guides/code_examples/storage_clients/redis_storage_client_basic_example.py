from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import RedisStorageClient

# Create a new instance of storage client using connection string.
# 'redis://localhost:6379' is the just placeholder, replace it with your actual
# connection string.
storage_client = RedisStorageClient(connection_string='redis://localhost:6379')

# And pass it to the crawler.
crawler = ParselCrawler(storage_client=storage_client)
