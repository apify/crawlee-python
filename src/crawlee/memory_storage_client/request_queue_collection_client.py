from __future__ import annotations

from typing_extensions import override

from crawlee.base_storage_client import BaseRequestQueueCollectionClient
from crawlee.memory_storage_client.base_resource_collection_client import (
    BaseResourceCollectionClient as BaseMemoryResourceCollectionClient,
)
from crawlee.memory_storage_client.request_queue_client import RequestQueueClient
from crawlee.storages.models import RequestQueuesListPage


class RequestQueueCollectionClient(BaseMemoryResourceCollectionClient, BaseRequestQueueCollectionClient):
    """Sub-client for manipulating request queues."""

    @property
    @override
    def _client_class(self) -> type[RequestQueueClient]:
        return RequestQueueClient

    @override
    def _get_storage_client_cache(self) -> list[RequestQueueClient]:
        return self._memory_storage_client.request_queues_handled

    @override
    async def list(
        self,
        *,
        unnamed: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool | None = None,
    ) -> RequestQueuesListPage:
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
