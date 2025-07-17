from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee.configuration import Configuration

    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._request_queue_client import RequestQueueClient


@docs_group('Abstract classes')
class StorageClient(ABC):
    """Base class for storage clients.

    The `StorageClient` serves as an abstract base class that defines the interface for accessing Crawlee's
    storage types: datasets, key-value stores, and request queues. It provides methods to open clients for
    each of these storage types and handles common functionality.

    Storage clients implementations can be provided for various backends (file system, memory, databases,
    various cloud providers, etc.) to support different use cases from development to production environments.

    Each storage client implementation is responsible for ensuring proper initialization, data persistence
    (where applicable), and consistent access patterns across all storage types it supports.
    """

    @abstractmethod
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> DatasetClient:
        """Create a dataset client."""

    @abstractmethod
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> KeyValueStoreClient:
        """Create a key-value store client."""

    @abstractmethod
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> RequestQueueClient:
        """Create a request queue client."""

    def get_rate_limit_errors(self) -> dict[int, int]:
        """Return statistics about rate limit errors encountered by the HTTP client in storage client."""
        return {}

    async def _purge_if_needed(
        self,
        client: DatasetClient | KeyValueStoreClient | RequestQueueClient,
        configuration: Configuration,
    ) -> None:
        """Purge the client if needed.

        The purge is only performed if the configuration indicates that it should be done and the client
        is not a named storage. Named storages are considered global and will typically outlive the run,
        so they are not purged.

        Args:
            client: The storage client to potentially purge.
            configuration: Configuration that determines whether purging should occur.
        """
        metadata = await client.get_metadata()
        if configuration.purge_on_start and metadata.name is None:
            await client.purge()
