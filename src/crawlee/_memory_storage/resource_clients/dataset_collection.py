from __future__ import annotations

from typing import TYPE_CHECKING

from apify_shared.utils import ignore_docs

from apify._memory_storage.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from apify._memory_storage.resource_clients.dataset import DatasetClient

if TYPE_CHECKING:
    from apify_shared.models import ListPage


@ignore_docs
class DatasetCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating datasets."""

    def _get_storage_client_cache(self: DatasetCollectionClient) -> list[DatasetClient]:
        return self._memory_storage_client._datasets_handled

    def _get_resource_client_class(self: DatasetCollectionClient) -> type[DatasetClient]:
        return DatasetClient

    async def list(self: DatasetCollectionClient) -> ListPage:
        """List the available datasets.

        Returns:
            ListPage: The list of available datasets matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self: DatasetCollectionClient,
        *,
        name: str | None = None,
        schema: dict | None = None,
        _id: str | None = None,
    ) -> dict:
        """Retrieve a named dataset, or create a new one when it doesn't exist.

        Args:
            name (str, optional): The name of the dataset to retrieve or create.
            schema (dict, optional): The schema of the dataset

        Returns:
            dict: The retrieved or newly-created dataset.
        """
        return await super().get_or_create(name=name, schema=schema, _id=_id)
