from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.resource_clients.request_queue_client import RequestQueueClient
from crawlee.storages.models import RequestQueuesListPage

if TYPE_CHECKING:
    from crawlee.storages.models import BaseStorageMetadata


class RequestQueueCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating request queues."""

    @property
    @override
    def _client_class(self) -> type[RequestQueueClient]:
        return RequestQueueClient

    @override
    def _get_storage_client_cache(self) -> list[RequestQueueClient]:
        return self._memory_storage_client.request_queues_handled

    @override
    async def list(self) -> RequestQueuesListPage:
        storage_client_cache = self._get_storage_client_cache()
        items = [storage.resource_info for storage in storage_client_cache]

        return RequestQueuesListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> BaseStorageMetadata:
        """Retrieve a named request queue, or create a new one when it doesn't exist.

        Args:
            name: The name of the request queue to retrieve or create.
            schema: The schema of the request queue
            id: The ID of the request queue to retrieve or create.

        Returns:
            The retrieved or newly-created request queue.
        """
        return await super().get_or_create(name=name, schema=schema, id=id)
