from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.resource_clients.dataset_client import DatasetClient

if TYPE_CHECKING:
    from crawlee.storages.types import BaseResourceInfo, ListPage


class DatasetCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating datasets."""

    @property
    @override
    def _client_class(self) -> type[DatasetClient]:
        return DatasetClient

    @override
    def _get_storage_client_cache(self) -> list[DatasetClient]:
        return self._memory_storage_client.datasets_handled

    async def list(self) -> ListPage:
        """List the available datasets.

        Returns:
            The list of available datasets matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> BaseResourceInfo:
        """Retrieve a named dataset, or create a new one when it doesn't exist.

        Args:
            name: The name of the dataset to retrieve or create.
            schema: The schema of the dataset
            id: ID of the dataset to retrieve or create

        Returns:
            The retrieved or newly-created dataset.
        """
        return await super().get_or_create(name=name, schema=schema, id=id)
