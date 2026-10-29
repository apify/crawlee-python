from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Hashable

    from crawlee.configuration import Configuration

    from ._dataset_backend import DatasetBackend
    from ._key_value_store_backend import KeyValueStoreBackend
    from ._request_queue_backend import RequestQueueBackend


@docs_group('Storage backends')
class StorageBackend(ABC):
    """Base class for storage backends.

    The `StorageBackend` serves as an abstract base class that defines the interface for accessing Crawlee's
    storage types: datasets, key-value stores, and request queues. It provides methods to open backends for
    each of these storage types and handles common functionality.

    Storage backend implementations can be provided for various systems (file system, memory, databases,
    various cloud providers, etc.) to support different use cases from development to production environments.

    Each storage backend implementation is responsible for ensuring proper initialization, data persistence
    (where applicable), and consistent access patterns across all storage types it supports.
    """

    def get_storage_backend_cache_key(self, configuration: Configuration) -> Hashable:  # noqa: ARG002
        """Return a cache key that can differentiate between different storages of this and other backends.

        Can be based on configuration or on the backend itself. By default, returns a module and name of the backend
        class.
        """
        return f'{self.__class__.__module__}.{self.__class__.__name__}'

    @abstractmethod
    async def create_dataset_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> DatasetBackend:
        """Create a dataset backend."""

    @abstractmethod
    async def create_kvs_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> KeyValueStoreBackend:
        """Create a key-value store backend."""

    @abstractmethod
    async def create_rq_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> RequestQueueBackend:
        """Create a request queue backend."""

    def get_rate_limit_errors(self) -> dict[int, int]:
        """Return statistics about rate limit errors encountered by the HTTP client in storage backend."""
        return {}

    async def _purge_if_needed(
        self,
        backend: DatasetBackend | KeyValueStoreBackend | RequestQueueBackend,
        configuration: Configuration,
    ) -> None:
        """Purge the storage behind the backend if needed.

        The purge is only performed if the configuration indicates that it should be done and the storage
        is not a named storage. Named storages are considered global and will typically outlive the run,
        so they are not purged.

        Args:
            backend: The dataset, key-value store, or request queue backend to potentially purge.
            configuration: Configuration that determines whether purging should occur.
        """
        metadata = await backend.get_metadata()
        if configuration.purge_on_start and metadata.name is None:
            await backend.purge()
