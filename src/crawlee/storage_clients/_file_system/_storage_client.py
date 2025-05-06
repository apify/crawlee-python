from __future__ import annotations

from typing_extensions import override

from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store_client import FileSystemKeyValueStoreClient
from ._request_queue_client import FileSystemRequestQueueClient


class FileSystemStorageClient(StorageClient):
    """File system storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await FileSystemDatasetClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start and client.metadata.name is None:
            await client.purge()

        return client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await FileSystemKeyValueStoreClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start and client.metadata.name is None:
            await client.purge()

        return client

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> FileSystemRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await FileSystemRequestQueueClient.open(id=id, name=name, configuration=configuration)

        if configuration.purge_on_start and client.metadata.name is None:
            await client.purge()

        return client
