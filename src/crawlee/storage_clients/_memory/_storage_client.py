from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MemoryDatasetClient
from ._key_value_store_client import MemoryKeyValueStoreClient
from ._request_queue_client import MemoryRequestQueueClient

if TYPE_CHECKING:
    from pathlib import Path


class MemoryStorageClient(StorageClient):
    """Memory storage client."""

    @override
    async def open_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None
    ) -> MemoryDatasetClient:
        client = await MemoryDatasetClient.open(id=id, name=name, storage_dir=storage_dir)

        if purge_on_start:
            await client.drop()
            client = await MemoryDatasetClient.open(id=id, name=name, storage_dir=storage_dir)

        return client

    @override
    async def open_key_value_store_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None
    ) -> MemoryKeyValueStoreClient:
        client = await MemoryKeyValueStoreClient.open(id=id, name=name, storage_dir=storage_dir)

        if purge_on_start:
            await client.drop()
            client = await MemoryKeyValueStoreClient.open(id=id, name=name, storage_dir=storage_dir)

        return client

    @override
    async def open_request_queue_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        purge_on_start: bool = True,
        storage_dir: Path | None = None
    ) -> MemoryRequestQueueClient:
        pass
