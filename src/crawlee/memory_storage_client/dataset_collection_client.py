from __future__ import annotations

from typing_extensions import override

from crawlee.base_storage_client import BaseDatasetCollectionClient
from crawlee.memory_storage_client.base_resource_collection_client import (
    BaseResourceCollectionClient as BaseMemoryResourceCollectionClient,
)
from crawlee.memory_storage_client.dataset_client import DatasetClient
from crawlee.storages.models import DatasetListPage


class DatasetCollectionClient(  # type: ignore
    BaseMemoryResourceCollectionClient,
    BaseDatasetCollectionClient,
):
    """Subclient for manipulating datasets."""

    @property
    @override
    def _client_class(self) -> type[DatasetClient]:
        return DatasetClient

    @override
    def _get_storage_client_cache(self) -> list[DatasetClient]:
        return self._memory_storage_client.datasets_handled

    @override
    async def list(
        self,
        *,
        unnamed: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool | None = None,
    ) -> DatasetListPage:
        storage_client_cache = self._get_storage_client_cache()
        items = [storage.resource_info for storage in storage_client_cache]

        return DatasetListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )
