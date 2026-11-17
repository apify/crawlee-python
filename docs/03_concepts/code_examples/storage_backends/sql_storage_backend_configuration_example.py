from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import SqlStorageBackend


async def main() -> None:
    # Create an `engine` with the desired configuration.
    engine = create_async_engine(
        'postgresql+asyncpg://myuser:mypassword@localhost:5432/postgres',
        future=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,
        pool_pre_ping=True,
        echo=False,
    )

    try:
        # Create a new instance of the storage backend.
        # On first run, also creates tables in your PostgreSQL database.
        async with SqlStorageBackend(engine=engine) as storage_backend:
            # Create a configuration with custom settings.
            configuration = Configuration(
                purge_on_start=False,
            )

            # And pass them to the crawler.
            crawler = ParselCrawler(
                storage_backend=storage_backend,
                configuration=configuration,
            )
    finally:
        # The backend doesn't dispose an engine you pass in, so dispose it yourself.
        await engine.dispose()
