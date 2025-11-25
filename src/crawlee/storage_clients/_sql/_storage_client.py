from __future__ import annotations

import warnings
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.sql import insert, select, text
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import SqlDatasetClient
from ._db_models import Base, VersionDb
from ._key_value_store_client import SqlKeyValueStoreClient
from ._request_queue_client import SqlRequestQueueClient

if TYPE_CHECKING:
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncSession


@docs_group('Storage clients')
class SqlStorageClient(StorageClient):
    """SQL implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that persist data
    to a SQL database using SQLAlchemy 2+. Each storage type uses two tables: one for metadata and one for
    records.

    The client accepts either a database connection string or a pre-configured AsyncEngine. If neither is
    provided, it creates a default SQLite database 'crawlee.db' in the storage directory.

    Database schema is automatically created during initialization. SQLite databases receive performance
    optimizations including WAL mode and increased cache size.

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.
    """

    _DEFAULT_DB_NAME = 'crawlee.db'
    """Default database name if not specified in connection string."""

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        engine: AsyncEngine | None = None,
    ) -> None:
        """Initialize the SQL storage client.

        Args:
            connection_string: Database connection string (e.g., "sqlite+aiosqlite:///crawlee.db").
                If not provided, defaults to SQLite database in the storage directory.
            engine: Pre-configured AsyncEngine instance. If provided, connection_string is ignored.
        """
        if engine is not None and connection_string is not None:
            raise ValueError('Either connection_string or engine must be provided, not both.')

        self._connection_string = connection_string
        self._engine = engine
        self._initialized = False
        self.session_maker: None | async_sessionmaker[AsyncSession] = None

        # Minimum interval to reduce database load from frequent concurrent metadata updates
        self._accessed_modified_update_interval = timedelta(seconds=1)

        # Flag needed to apply optimizations only for default database
        self._default_flag = self._engine is None and self._connection_string is None
        self._dialect_name: str | None = None

        # Call the notification only once
        warnings.warn(
            'The SqlStorageClient is experimental and may change or be removed in future releases.',
            category=UserWarning,
            stacklevel=2,
        )

    async def __aenter__(self) -> SqlStorageClient:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def engine(self) -> AsyncEngine:
        """Get the SQLAlchemy AsyncEngine instance."""
        if self._engine is None:
            raise ValueError('Engine is not initialized. Call initialize() before accessing the engine.')
        return self._engine

    def get_dialect_name(self) -> str | None:
        """Get the database dialect name."""
        return self._dialect_name

    def get_accessed_modified_update_interval(self) -> timedelta:
        """Get the interval for accessed and modified updates."""
        return self._accessed_modified_update_interval

    async def initialize(self, configuration: Configuration) -> None:
        """Initialize the database schema.

        This method creates all necessary tables if they don't exist.
        Should be called before using the storage client.
        """
        if not self._initialized:
            engine = self._get_or_create_engine(configuration)
            async with engine.begin() as conn:
                self._dialect_name = engine.dialect.name

                if self._dialect_name not in ('sqlite', 'postgresql'):
                    raise ValueError(
                        f'Unsupported database dialect: {self._dialect_name}. Supported: sqlite, postgresql. '
                        'Consider using a different database.',
                    )

                # Create tables if they don't exist.
                # Rollback the transaction when an exception occurs.
                # This is likely an attempt to create a database from several parallel processes.
                try:
                    # Set SQLite pragmas for performance and consistency
                    if self._default_flag:
                        await conn.execute(text('PRAGMA journal_mode=WAL'))  # Better concurrency
                        await conn.execute(text('PRAGMA synchronous=NORMAL'))  # Balanced safety/speed
                        await conn.execute(text('PRAGMA cache_size=100000'))  # 100MB cache
                        await conn.execute(text('PRAGMA temp_store=MEMORY'))  # Memory temp storage
                        await conn.execute(text('PRAGMA mmap_size=268435456'))  # 256MB memory mapping
                        await conn.execute(text('PRAGMA foreign_keys=ON'))  # Enforce constraints
                        await conn.execute(text('PRAGMA busy_timeout=30000'))  # 30s busy timeout

                    await conn.run_sync(Base.metadata.create_all, checkfirst=True)

                    from crawlee import __version__  # Noqa: PLC0415

                    db_version = (await conn.execute(select(VersionDb))).scalar_one_or_none()

                    # Raise an error if the new version creates breaking changes in the database schema.
                    if db_version and db_version != __version__:
                        warnings.warn(
                            f'Database version {db_version} does not match library version {__version__}. '
                            'This may lead to unexpected behavior. Drop the db if you want to make sure that '
                            'everything will work fine.',
                            category=UserWarning,
                            stacklevel=2,
                        )
                    elif not db_version:
                        await conn.execute(insert(VersionDb).values(version=__version__))

                except (IntegrityError, OperationalError):
                    await conn.rollback()

            self._initialized = True

    async def close(self) -> None:
        """Close the database connection pool."""
        if self._engine is not None:
            await self._engine.dispose()
        self._engine = None

    def create_session(self) -> AsyncSession:
        """Create a new database session.

        Returns:
            A new AsyncSession instance.
        """
        if self.session_maker is None:
            self.session_maker = async_sessionmaker(self._engine, expire_on_commit=False, autoflush=False)
        return self.session_maker()

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> SqlDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self.initialize(configuration)

        client = await SqlDatasetClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_client=self,
        )

        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> SqlKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self.initialize(configuration)

        client = await SqlKeyValueStoreClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_client=self,
        )

        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> SqlRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self.initialize(configuration)

        client = await SqlRequestQueueClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_client=self,
        )

        await self._purge_if_needed(client, configuration)
        return client

    def _get_or_create_engine(self, configuration: Configuration) -> AsyncEngine:
        """Get or create the database engine based on configuration."""
        if self._engine is not None:
            return self._engine

        if self._connection_string is not None:
            connection_string = self._connection_string
        else:
            # Create SQLite database in the storage directory
            storage_dir = Path(configuration.storage_dir)
            if not storage_dir.exists():
                storage_dir.mkdir(parents=True, exist_ok=True)

            db_path = storage_dir / self._DEFAULT_DB_NAME

            # Create connection string with path to default database
            connection_string = f'sqlite+aiosqlite:///{db_path}'

        if 'sqlite' not in connection_string and 'postgresql' not in connection_string:
            raise ValueError(
                'Unsupported database. Supported: sqlite, postgresql. Consider using a different database.'
            )

        self._engine = create_async_engine(
            connection_string,
            future=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=600,
            pool_pre_ping=True,
            echo=False,
            connect_args={'timeout': 30},
        )
        return self._engine
