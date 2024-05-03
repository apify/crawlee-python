from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.storages_v2.base_storage import BaseStorage

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client.base_dataset_client import BaseDatasetClient
    from crawlee.base_storage_client.base_dataset_collection_client import BaseDatasetCollectionClient
    from crawlee.configuration import Configuration


class Dataset(BaseStorage):
    """A class for managing datasets."""

    LABEL = 'Dataset'

    def __init__(
        self,
        id: str,
        name: str | None,
        configuration: Configuration,
        client: BaseStorageClient,
    ) -> None:
        self._id = id
        self._name = name
        self._configuration = configuration
        self._client = client

    @property
    def _resource_client(self) -> BaseDatasetClient:
        return self._client.dataset(self._id)

    @property
    def _collection_storage_client(self) -> BaseDatasetCollectionClient:
        return self._client.datasets()
