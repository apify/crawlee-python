from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store_client import FileSystemKeyValueStoreClient
from ._request_queue_client import FileSystemRequestQueueClient


@docs_group('Classes')
class FileSystemStorageClient(StorageClient):
    """File system implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that persist data
    to the local file system. Each storage type is implemented with its own specific file system client
    that stores data in a structured directory hierarchy.

    Data is stored in JSON format in predictable file paths, making it easy to inspect and manipulate
    the stored data outside of the Crawlee application if needed.

    All data persists between program runs but is limited to access from the local machine
    where the files are stored.
    """

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
        await self._purge_if_needed(client, configuration)
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
        await self._purge_if_needed(client, configuration)
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
        await self._purge_if_needed(client, configuration)
        return client
