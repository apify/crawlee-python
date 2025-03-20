from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storage_clients._base import StorageClient

from ._dataset_client import FileSystemDatasetClient
from ._key_value_store import FileSystemKeyValueStoreClient
from ._request_queue import FileSystemRequestQueueClient

if TYPE_CHECKING:
    from pathlib import Path


class FileSystemStorageClient(StorageClient):
    """File system storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None,
        name: str | None,
        purge_on_start: bool,
        storage_dir: Path,
    ) -> FileSystemDatasetClient:
        dataset_client = await FileSystemDatasetClient.open(
            id=id,
            name=name,
            storage_dir=storage_dir,
        )

        if purge_on_start:
            await dataset_client.drop()
            dataset_client = await FileSystemDatasetClient.open(
                id=id,
                name=name,
                storage_dir=storage_dir,
            )

        return dataset_client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None,
        name: str | None,
        purge_on_start: bool,
        storage_dir: Path,
    ) -> FileSystemKeyValueStoreClient:
        return FileSystemKeyValueStoreClient()

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None,
        name: str | None,
        purge_on_start: bool,
        storage_dir: Path,
    ) -> FileSystemRequestQueueClient:
        return FileSystemRequestQueueClient()
