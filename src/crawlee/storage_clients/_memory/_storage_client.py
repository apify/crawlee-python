from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MemoryDatasetClient
from ._key_value_store_client import MemoryKeyValueStoreClient
from ._request_queue_client import MemoryRequestQueueClient


@docs_group('Storage clients')
class MemoryStorageClient(StorageClient):
    """Memory implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that store all data
    in memory using Python data structures (lists and dictionaries). No data is persisted between process runs,
    meaning all stored data is lost when the program terminates.

    The memory implementation provides fast access to data but is limited by available memory and does not
    support data sharing across different processes. All storage operations happen entirely in memory with
    no disk operations.

    The memory storage client is useful for testing and development environments, or short-lived crawler
    operations where persistence is not required.
    """

    def __init__(self, configuration: Configuration | None = None) -> None:
        """Initialize the file system storage client.

        Args:
            configuration: Optional configuration instance to use with the storage client.
                If not provided, the global configuration will be used.
        """
        self._configuration = configuration or Configuration.get_global_configuration()

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> MemoryDatasetClient:
        client = await MemoryDatasetClient.open(id=id, name=name)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> MemoryKeyValueStoreClient:
        client = await MemoryKeyValueStoreClient.open(id=id, name=name)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> MemoryRequestQueueClient:
        client = await MemoryRequestQueueClient.open(id=id, name=name)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    def create_client(self, configuration: Configuration) -> MemoryStorageClient:
        """Create a storage client from an existing storage client potentially just replacing the configuration."""
        return MemoryStorageClient(configuration or self._configuration)
