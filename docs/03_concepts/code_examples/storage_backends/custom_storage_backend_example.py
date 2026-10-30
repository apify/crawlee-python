from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.storage_backends import StorageBackend
from crawlee.storage_backends._base import (
    DatasetBackend,
    KeyValueStoreBackend,
    RequestQueueBackend,
)

if TYPE_CHECKING:
    from crawlee.configuration import Configuration

# Implement the storage type backends with your backend logic.


class CustomDatasetBackend(DatasetBackend):
    # Implement methods like push_data, get_data, iterate_items, etc.
    pass


class CustomKeyValueStoreBackend(KeyValueStoreBackend):
    # Implement methods like get_value, set_value, delete, etc.
    pass


class CustomRequestQueueBackend(RequestQueueBackend):
    # Implement methods like add_request, fetch_next_request, etc.
    pass


# Implement the storage backend factory.


class CustomStorageBackend(StorageBackend):
    async def create_dataset_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomDatasetBackend:
        # Create and return your custom dataset backend.
        pass

    async def create_kvs_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomKeyValueStoreBackend:
        # Create and return your custom key-value store backend.
        pass

    async def create_rq_backend(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> CustomRequestQueueBackend:
        # Create and return your custom request queue backend.
        pass
