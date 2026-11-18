from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Literal

from redis.asyncio import Redis
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_backends._base import StorageBackend

from ._dataset_backend import RedisDatasetBackend
from ._key_value_store_backend import RedisKeyValueStoreBackend
from ._request_queue_backend import RedisRequestQueueBackend

if TYPE_CHECKING:
    from collections.abc import Hashable
    from types import TracebackType
    from typing import Self


@docs_group('Storage backends')
class RedisStorageBackend(StorageBackend):
    """Redis implementation of the storage backend.

    This storage backend provides access to datasets, key-value stores, and request queues that persist data
    to a Redis database v8.0+. Each storage type uses Redis-specific data structures and key patterns for
    efficient storage and retrieval.

    The backend accepts either a Redis connection string or a pre-configured Redis client instance. Exactly one of these
    parameters must be provided during initialization. Key-value stores hold binary values, so their backend needs
    a Redis client that returns raw bytes, which is the default; a client created with `decode_responses=True` works
    for datasets and request queues only.

    Storage types use the following Redis data structures:
    - **Datasets**: Redis JSON arrays for item storage with metadata in JSON objects
    - **Key-value stores**: Redis hashes for key-value pairs with separate metadata storage
    - **Request queues**: Redis lists for FIFO queuing, hashes for request data and in-progress tracking,
      and Bloom filters for request deduplication

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.
    """

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        redis: Redis | None = None,
        queue_dedup_strategy: Literal['default', 'bloom'] = 'default',
        queue_bloom_error_rate: float = 1e-7,
    ) -> None:
        """Initialize the Redis storage backend.

        Args:
            connection_string: Redis connection string (e.g., "redis://localhost:6379").
                Supports standard Redis URL format with optional database selection.
            redis: Pre-configured Redis client instance. The backend doesn't close it.
            queue_dedup_strategy: Strategy for request queue deduplication. Options are:
                - 'default': Uses Redis sets for exact deduplication.
                - 'bloom': Uses Redis Bloom filters for probabilistic deduplication with lower memory usage. When using
                    this approach, approximately 1 in 1e-7 requests will be falsely considered duplicate.
            queue_bloom_error_rate: Desired false positive rate for Bloom filter deduplication. Only relevant if
                `queue_dedup_strategy` is set to 'bloom'.
        """
        if redis is None and connection_string is None:
            raise ValueError('Either redis or connection_string must be provided.')

        if redis is not None and connection_string is not None:
            raise ValueError('Either redis or connection_string must be provided, not both.')

        if isinstance(redis, Redis) and connection_string is None:
            self._redis = redis

        if isinstance(connection_string, str) and redis is None:
            self._redis = Redis.from_url(connection_string)

        self._redis: Redis  # to help type checker
        self._owns_redis = redis is None
        self._closed = False
        self._queue_dedup_strategy = queue_dedup_strategy
        self._queue_bloom_error_rate = queue_bloom_error_rate

        # Call the notification only once
        warnings.warn(
            (
                'RedisStorageBackend is experimental and its API, behavior, and key structure may change in future '
                'releases.'
            ),
            category=UserWarning,
            stacklevel=2,
        )

    async def __aenter__(self) -> Self:
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
    def redis(self) -> Redis:
        """The Redis client of the storage backend.

        Raises:
            RuntimeError: If the storage backend is closed.
        """
        # A closed client would silently open new connections that nobody closes.
        if self._closed:
            raise RuntimeError(f'The {self.__class__.__name__} is closed.')

        return self._redis

    async def close(self) -> None:
        """Close the Redis client, unless it was passed in.

        The storage backend and the storages opened with it can't be used afterwards.
        """
        if self._closed:
            return

        self._closed = True

        if self._owns_redis:
            await self._redis.aclose()

    @override
    def get_storage_backend_cache_key(self, configuration: Configuration) -> Hashable:
        # Storages stop working when their backend closes, so a different backend instance must not get them.
        return super().get_storage_backend_cache_key(configuration), id(self)

    @override
    async def create_dataset_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisDatasetBackend:
        configuration = configuration or Configuration.get_global_configuration()

        backend = await RedisDatasetBackend.open(
            id=id,
            name=name,
            alias=alias,
            storage_backend=self,
        )

        await self._purge_if_needed(backend, configuration)
        return backend

    @override
    async def create_kvs_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisKeyValueStoreBackend:
        configuration = configuration or Configuration.get_global_configuration()

        backend = await RedisKeyValueStoreBackend.open(
            id=id,
            name=name,
            alias=alias,
            storage_backend=self,
        )

        await self._purge_if_needed(backend, configuration)
        return backend

    @override
    async def create_rq_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> RedisRequestQueueBackend:
        configuration = configuration or Configuration.get_global_configuration()

        backend = await RedisRequestQueueBackend.open(
            id=id,
            name=name,
            alias=alias,
            storage_backend=self,
            dedup_strategy=self._queue_dedup_strategy,
            bloom_error_rate=self._queue_bloom_error_rate,
        )

        await self._purge_if_needed(backend, configuration)
        return backend
