from __future__ import annotations

from typing_extensions import override

from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MemoryDatasetClient
from ._key_value_store_client import MemoryKeyValueStoreClient
from ._request_queue_client import MemoryRequestQueueClient


class MemoryStorageClient(StorageClient):
    """Memory storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryDatasetClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.purge()

        return client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryKeyValueStoreClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.purge()

        return client

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> MemoryRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await MemoryRequestQueueClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start:
            await client.purge()

        return client
