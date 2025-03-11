# Inspiration: https://github.com/apify/crawlee/blob/v3.8.2/packages/types/src/storages.ts#L314:L328

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from ._dataset_client import DatasetClient
    from ._key_value_store_client import KeyValueStoreClient
    from ._request_queue_client import RequestQueueClient


@docs_group('Abstract classes')
class StorageClient(ABC):
    """Defines an abstract base for storage clients.

    It offers interfaces to get subclients for interacting with storage resources like datasets, key-value stores,
    and request queues.
    """

    @abstractmethod
    def dataset(self, id: str) -> DatasetClient:
        """Get a subclient for a specific dataset by its ID."""

    @abstractmethod
    def key_value_store(self, id: str) -> KeyValueStoreClient:
        """Get a subclient for a specific key-value store by its ID."""

    @abstractmethod
    def request_queue(self, id: str) -> RequestQueueClient:
        """Get a subclient for a specific request queue by its ID."""

    @abstractmethod
    async def purge_on_start(self) -> None:
        """Perform a purge of the default storages.

        This method ensures that the purge is executed only once during the lifetime of the instance.
        It is primarily used to clean up residual data from previous runs to maintain a clean state.
        If the storage client does not support purging, leave it empty.
        """

    def get_rate_limit_errors(self) -> dict[int, int]:
        """Return statistics about rate limit errors encountered by the HTTP client in storage client."""
        return {}
