# Inspiration: https://github.com/apify/crawlee/blob/v3.8.2/packages/types/src/storages.ts#L314:L328

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base_dataset_client import BaseDatasetClient
    from .base_dataset_collection_client import BaseDatasetCollectionClient
    from .base_key_value_store_client import BaseKeyValueStoreClient
    from .base_key_value_store_collection_client import BaseKeyValueStoreCollectionClient
    from .base_request_queue_client import BaseRequestQueueClient
    from .base_request_queue_collection_client import BaseRequestQueueCollectionClient


class BaseStorageClient(ABC):
    """Defines an abstract base for storage clients.

    It offers interfaces to get clients for interacting with storage resources like datasets, key-value stores,
    and request queues.
    """

    @abstractmethod
    def dataset(self, id: str) -> BaseDatasetClient:
        """Gets a client for a specific dataset by its ID."""

    @abstractmethod
    def datasets(self) -> BaseDatasetCollectionClient:
        """Gets a client for dataset collection operations."""

    @abstractmethod
    def key_value_store(self, id: str) -> BaseKeyValueStoreClient:
        """Gets a client for a specific key-value store by its ID."""

    @abstractmethod
    def key_value_stores(self) -> BaseKeyValueStoreCollectionClient:
        """Gets a client for key-value store collection operations."""

    @abstractmethod
    def request_queue(self, id: str) -> BaseRequestQueueClient:
        """Gets a client for a specific request queue by its ID."""

    @abstractmethod
    def request_queues(self) -> BaseRequestQueueCollectionClient:
        """Gets a client for request queue collection operations."""
