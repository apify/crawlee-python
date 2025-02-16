from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.storage_clients._base import KeyValueStoreCollectionClient as BaseKeyValueStoreCollectionClient
from crawlee.storage_clients.models import KeyValueStoreListPage, KeyValueStoreMetadata

from ._creation_management import get_or_create_inner
from ._key_value_store_client import KeyValueStoreClient

if TYPE_CHECKING:
    from ._memory_storage_client import MemoryStorageClient


class KeyValueStoreCollectionClient(BaseKeyValueStoreCollectionClient):
    """Subclient for manipulating key-value stores."""

    def __init__(self, *, memory_storage_client: MemoryStorageClient) -> None:
        self._memory_storage_client = memory_storage_client

    @property
    def _storage_client_cache(self) -> list[KeyValueStoreClient]:
        return self._memory_storage_client.key_value_stores_handled

    @override
    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id: str | None = None,
    ) -> KeyValueStoreMetadata:
        resource_client = await get_or_create_inner(
            memory_storage_client=self._memory_storage_client,
            storage_client_cache=self._storage_client_cache,
            resource_client_class=KeyValueStoreClient,
            name=name,
            id=id,
        )
        return resource_client.resource_info

    @override
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> KeyValueStoreListPage:
        items = [storage.resource_info for storage in self._storage_client_cache]

        return KeyValueStoreListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )
