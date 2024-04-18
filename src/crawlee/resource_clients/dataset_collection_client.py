from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.resource_clients.dataset_client import DatasetClient
from crawlee.storages.models import DatasetsListPage

if TYPE_CHECKING:
    from crawlee.storages.models import BaseStorageMetadata


class DatasetCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating datasets."""

    @property
    @override
    def _client_class(self) -> type[DatasetClient]:
        return DatasetClient

    @override
    def _get_storage_client_cache(self) -> list[DatasetClient]:
        return self._memory_storage_client.datasets_handled

    @override
    async def list(self) -> DatasetsListPage:
        storage_client_cache = self._get_storage_client_cache()
        items = [storage.resource_info for storage in storage_client_cache]

        return DatasetsListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> BaseStorageMetadata:
        """Retrieve a named dataset, or create a new one when it doesn't exist.

        Args:
            name: The name of the dataset to retrieve or create.
            schema: The schema of the dataset
            id: ID of the dataset to retrieve or create

        Returns:
            The retrieved or newly-created dataset.
        """
        return await super().get_or_create(name=name, schema=schema, id=id)
