from __future__ import annotations

from redis.asyncio import Redis
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import RedisDatasetClient
from ._key_value_store_client import RedisKeyValueStoreClient
from ._request_queue_client import RedisRequestQueueClient


@docs_group('Storage clients')
class RedisStorageClient(StorageClient):
    """Redis implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that persist data
    to a Redis database. Each storage type uses a different key pattern to store and retrieve data.

    The client accepts either a database connection string or a pre-configured AsyncEngine. If neither is
    provided, it creates a default SQLite database 'crawlee.db' in the storage directory.

    Database schema is automatically created during initialization. SQLite databases receive performance
    optimizations including WAL mode and increased cache size.

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.
    """

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        redis: Redis | None = None,
    ) -> None:
        """Initialize the SQL storage client.

        Args:
            connection_string: Database connection string.
            redis: Pre-configured Redis client instance.
        """
        if redis is not None and connection_string is not None:
            raise ValueError('Either redis or connection_string must be provided, not both.')

        if redis is None and connection_string is None:
            raise ValueError('Either redis or connection_string must be provided.')

        if redis is not None:
            self._redis = redis

        elif connection_string is not None:
            self._redis = Redis.from_url(connection_string)

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisDatasetClient:
        """Create or open a Redis dataset client.

        Args:
            id: Specific dataset ID to open. If provided, name is ignored.
            name: Dataset name to open or create. Uses 'default' if not specified.
            configuration: Configuration object. Uses global config if not provided.

        Returns:
            Configured dataset client ready for use.
        """
        configuration = configuration or Configuration.get_global_configuration()

        client = await RedisDatasetClient.open(
            id=id,
            name=name,
            redis=self._redis,
        )

        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisKeyValueStoreClient:
        """Create or open a SQL key-value store client.

        Args:
            id: Specific store ID to open. If provided, name is ignored.
            name: Store name to open or create. Uses 'default' if not specified.
            configuration: Configuration object. Uses global config if not provided.

        Returns:
            Configured key-value store client ready for use.
        """
        configuration = configuration or Configuration.get_global_configuration()

        client = await RedisKeyValueStoreClient.open(
            id=id,
            name=name,
            redis=self._redis,
        )

        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisRequestQueueClient:
        """Create or open a SQL request queue client.

        Args:
            id: Specific queue ID to open. If provided, name is ignored.
            name: Queue name to open or create. Uses 'default' if not specified.
            configuration: Configuration object. Uses global config if not provided.

        Returns:
            Configured request queue client ready for use.
        """
        configuration = configuration or Configuration.get_global_configuration()

        client = await RedisRequestQueueClient.open(
            id=id,
            name=name,
            redis=self._redis,
        )

        await self._purge_if_needed(client, configuration)
        return client
