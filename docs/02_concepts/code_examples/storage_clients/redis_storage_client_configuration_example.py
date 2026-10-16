from redis.asyncio import Redis

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import RedisStorageClient

# Create a new instance of storage client using a Redis client with custom settings.
# Replace host and port with your actual Redis server configuration.
# Other Redis client settings can be adjusted as needed.
storage_client = RedisStorageClient(
    redis=Redis(
        host='localhost',
        port=6379,
        retry_on_timeout=True,
        socket_keepalive=True,
        socket_connect_timeout=10,
    )
)

# Create a configuration with custom settings.
configuration = Configuration(purge_on_start=False)

# And pass them to the crawler.
crawler = ParselCrawler(
    storage_client=storage_client,
    configuration=configuration,
)
