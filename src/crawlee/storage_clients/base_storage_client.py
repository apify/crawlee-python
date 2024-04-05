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
    def dataset(self, id_: str) -> DatasetClient:
        """Gets a client for a specific dataset by its ID."""

    @abstractmethod
    def datasets(self) -> DatasetCollectionClient:
        """Gets a client for dataset collection operations."""

    @abstractmethod
    def key_value_store(self, id_: str) -> KeyValueStoreClient:
        """Gets a client for a specific key-value store by its ID."""

    @abstractmethod
    def key_value_stores(self) -> KeyValueStoreCollectionClient:
        """Gets a client for key-value store collection operations."""

    @abstractmethod
    def request_queue(self, id_: str) -> RequestQueueClient:
        """Gets a client for a specific request queue by its ID."""

    @abstractmethod
    def request_queues(self) -> RequestQueueCollectionClient:
        """Gets a client for request queue collection operations."""

    # @abstractmethod
    # async def purge(self) -> None:
    #     pass

    # @abstractmethod
    # def teardown(self) -> None:
    #     pass

    # @abstractmethod
    # def set_status_message(self, message: str) -> None:
    #     pass

    # @abstractmethod
    # def stats(self) -> dict:
    #     pass
