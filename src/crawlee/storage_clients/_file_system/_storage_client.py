from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store_client import FileSystemKeyValueStoreClient
from ._request_queue_client import FileSystemRequestQueueClient

if TYPE_CHECKING:
    from pathlib import Path


class FileSystemStorageClient(StorageClient):
    """File system storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> FileSystemDatasetClient:
        client = await FileSystemDatasetClient.open(id=id, name=name, storage_dir=storage_dir)

        if purge_on_start:
            await client.drop()
            client = await FileSystemDatasetClient.open(id=id, name=name, storage_dir=storage_dir)

        return client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> FileSystemKeyValueStoreClient:
        client = await FileSystemKeyValueStoreClient.open(id=id, name=name, storage_dir=storage_dir)

        if purge_on_start:
            await client.drop()
            client = await FileSystemKeyValueStoreClient.open(id=id, name=name, storage_dir=storage_dir)

        return client

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None,
    ) -> FileSystemRequestQueueClient:
        pass
