from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.base_storage_client import BaseRequestQueueCollectionClient
from crawlee.memory_storage_client._creation_management import get_or_create_inner
from crawlee.memory_storage_client.request_queue_client import RequestQueueClient
from crawlee.storages.models import RequestQueueListPage, RequestQueueMetadata

if TYPE_CHECKING:
    from crawlee.memory_storage_client.memory_storage_client import MemoryStorageClient


class RequestQueueCollectionClient(BaseRequestQueueCollectionClient):
    """Subclient for manipulating request queues."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client

    @property
    def _storage_client_cache(self) -> list[RequestQueueClient]:
        return self._memory_storage_client.request_queues_handled

    @override
    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> RequestQueueMetadata:
        return await get_or_create_inner(
            memory_storage_client=self._memory_storage_client,
            base_storage_directory=self._base_storage_directory,
            storage_client_cache=self._storage_client_cache,
            resource_client_class=RequestQueueClient,
            name=name,
            id=id,
        )

    @override
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> RequestQueueListPage:
        items = [storage.resource_info for storage in self._storage_client_cache]

        return RequestQueueListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )
