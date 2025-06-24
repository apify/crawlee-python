from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.storage_clients import StorageClient
from crawlee.storage_clients._base import (
    DatasetClient,
    KeyValueStoreClient,
    RequestQueueClient,
)

if TYPE_CHECKING:
    from crawlee.configuration import Configuration

# Implement the storage type clients with your backend logic.


class CustomDatasetClient(DatasetClient):
    # Implement methods like push_data, get_data, iterate_items, etc.
    pass


class CustomKeyValueStoreClient(KeyValueStoreClient):
    # Implement methods like get_value, set_value, delete, etc.
    pass


class CustomRequestQueueClient(RequestQueueClient):
    # Implement methods like add_request, fetch_next_request, etc.
    pass


# Implement the storage client factory.


class CustomStorageClient(StorageClient):
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomDatasetClient:
        # Create and return your custom dataset client.
        pass

    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomKeyValueStoreClient:
        # Create and return your custom key-value store client.
        pass

    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomRequestQueueClient:
        # Create and return your custom request queue client.
        pass
