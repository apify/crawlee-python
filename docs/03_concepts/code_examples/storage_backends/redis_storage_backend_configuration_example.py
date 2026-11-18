from redis.asyncio import Redis

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import RedisStorageBackend


async def main() -> None:
    # Create a Redis client with custom settings.
    # Replace host and port with your actual Redis server configuration.
    # Other Redis client settings can be adjusted as needed.
    redis = Redis(
        host='localhost',
        port=6379,
        retry_on_timeout=True,
        socket_keepalive=True,
        socket_connect_timeout=10,
    )

    # The storage backend doesn't close a client you pass in, so close it yourself.
    async with redis, RedisStorageBackend(redis=redis) as storage_backend:
        # Create a configuration with custom settings.
        configuration = Configuration(purge_on_start=False)

        # And pass them to the crawler.
        crawler = ParselCrawler(
            storage_backend=storage_backend,
            configuration=configuration,
        )

        # Run the crawler inside the block, while the storage backend is open.
        await crawler.run(['https://crawlee.dev'])
