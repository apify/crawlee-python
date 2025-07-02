from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MemoryDatasetClient
from ._key_value_store_client import MemoryKeyValueStoreClient
from ._request_queue_client import MemoryRequestQueueClient


@docs_group('Classes')
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

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryDatasetClient.open(id=id, name=name)
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryKeyValueStoreClient.open(id=id, name=name)
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryRequestQueueClient.open(id=id, name=name)
        await self._purge_if_needed(client, configuration)
        return client
