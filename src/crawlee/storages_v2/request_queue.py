from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.base_storage_client.base_request_queue_collection_client import BaseRequestQueueCollectionClient
from crawlee.storages_v2.base_storage import BaseStorage

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.base_storage_client.base_request_queue_client import BaseRequestQueueClient
    from crawlee.base_storage_client.base_request_queue_collection_client import BaseRequestQueueCollectionClient
    from crawlee.configuration import Configuration


class RequestQueue(BaseStorage):
    """A class for managing request queues."""

    LABEL = 'Request queue'

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
    def _resource_client(self) -> BaseRequestQueueClient:
        return self._client.request_queue(self._id)

    @property
    def _collection_storage_client(self) -> BaseRequestQueueCollectionClient:
        return self._client.request_queues()
