from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.memory_storage.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.memory_storage.request_queue_client import RequestQueueClient

if TYPE_CHECKING:
    from crawlee.storages.types import BaseResourceInfo, ListPage


class RequestQueueCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating request queues."""

    @property
    @override
    def _client_class(self) -> type[RequestQueueClient]:
        return RequestQueueClient

    @override
    def _get_storage_client_cache(self) -> list[RequestQueueClient]:
        return self._memory_storage_client.request_queues_handled

    async def list(self) -> ListPage:
        """List the available request queues.

        Returns:
            The list of available request queues matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id_: str | None = None,
    ) -> BaseResourceInfo:
        """Retrieve a named request queue, or create a new one when it doesn't exist.

        Args:
            name: The name of the request queue to retrieve or create.
            schema: The schema of the request queue
            id_: The ID of the request queue to retrieve or create.

        Returns:
            The retrieved or newly-created request queue.
        """
        return await super().get_or_create(name=name, schema=schema, id_=id_)
