from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storage_clients._base import DatasetCollectionClient as BaseDatasetCollectionClient
from crawlee.storage_clients.models import DatasetListPage, DatasetMetadata

from ._creation_management import get_or_create_inner
from ._dataset_client import DatasetClient

if TYPE_CHECKING:
    from ._memory_storage_client import MemoryStorageClient


class DatasetCollectionClient(BaseDatasetCollectionClient):
    """Subclient for manipulating datasets."""

    def __init__(self, *, memory_storage_client: MemoryStorageClient) -> None:
        self._memory_storage_client = memory_storage_client

    @property
    def _storage_client_cache(self) -> list[DatasetClient]:
        return self._memory_storage_client.datasets_handled

    @override
    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> DatasetMetadata:
        resource_client = await get_or_create_inner(
            memory_storage_client=self._memory_storage_client,
            storage_client_cache=self._storage_client_cache,
            resource_client_class=DatasetClient,
            name=name,
            id=id,
        )
        return resource_client.resource_info

    @override
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> DatasetListPage:
        items = [storage.resource_info for storage in self._storage_client_cache]

        return DatasetListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )
