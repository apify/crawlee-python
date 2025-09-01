from __future__ import annotations

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store_client import FileSystemKeyValueStoreClient
from ._request_queue_client import FileSystemRequestQueueClient


@docs_group('Storage clients')
class FileSystemStorageClient(StorageClient):
    """File system implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that persist data
    to the local file system. Each storage type is implemented with its own specific file system client
    that stores data in a structured directory hierarchy.

    Data is stored in JSON format in predictable file paths, making it easy to inspect and manipulate
    the stored data outside of the Crawlee application if needed.

    All data persists between program runs but is limited to access from the local machine
    where the files are stored.

    Warning: This storage client is not safe for concurrent access from multiple crawler processes.
    Use it only when running a single crawler process at a time.
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
    ) -> FileSystemDatasetClient:
        client = await FileSystemDatasetClient.open(id=id, name=name, configuration=self._configuration)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> FileSystemKeyValueStoreClient:
        client = await FileSystemKeyValueStoreClient.open(id=id, name=name, configuration=self._configuration)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
    ) -> FileSystemRequestQueueClient:
        client = await FileSystemRequestQueueClient.open(id=id, name=name, configuration=self._configuration)
        await self._purge_if_needed(client, self._configuration)
        return client

    @override
    def create_client(self, configuration: Configuration) -> FileSystemStorageClient:
        """Create a storage client from an existing storage client potentially just replacing the configuration."""
        return FileSystemStorageClient(configuration)
