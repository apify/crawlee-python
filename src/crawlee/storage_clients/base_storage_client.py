# Inspiration: https://github.com/apify/crawlee/blob/v3.8.2/packages/types/src/storages.ts#L314:L328

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.resource_clients import (
        DatasetClient,
        DatasetCollectionClient,
        KeyValueStoreClient,
        KeyValueStoreCollectionClient,
        RequestQueueClient,
        RequestQueueCollectionClient,
    )


class BaseStorageClient(ABC):
    """Defines an abstract base for storage clients.

    It offers interfaces to get clients for interacting with storage resources like datasets, key-value stores,
    and request queues.
    """

    @abstractmethod
    def dataset(self, id: str) -> DatasetClient:
        """Gets a client for a specific dataset by its ID."""

    @abstractmethod
    def datasets(self) -> DatasetCollectionClient:
        """Gets a client for dataset collection operations."""

    @abstractmethod
    def key_value_store(self, id: str) -> KeyValueStoreClient:
        """Gets a client for a specific key-value store by its ID."""

    @abstractmethod
    def key_value_stores(self) -> KeyValueStoreCollectionClient:
        """Gets a client for key-value store collection operations."""

    @abstractmethod
    def request_queue(self, id: str) -> RequestQueueClient:
        """Gets a client for a specific request queue by its ID."""

    @abstractmethod
    def request_queues(self) -> RequestQueueCollectionClient:
        """Gets a client for request queue collection operations."""
