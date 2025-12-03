from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import SqlStorageClient


async def main() -> None:
    # Create a new instance of storage client.
    # On first run, also creates tables in your PostgreSQL database.
    # Use the context manager to ensure that connections are properly cleaned up.
    async with SqlStorageClient(
        # Create an `engine` with the desired configuration
        engine=create_async_engine(
            'postgresql+asyncpg://myuser:mypassword@localhost:5432/postgres',
            future=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=False,
        )
    ) as storage_client:
        # Create a configuration with custom settings.
        configuration = Configuration(
            purge_on_start=False,
        )

        # And pass them to the crawler.
        crawler = ParselCrawler(
            storage_client=storage_client,
            configuration=configuration,
        )
