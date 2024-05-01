from __future__ import annotations

from typing_extensions import override

from crawlee.base_storage_client import BaseKeyValueStoreCollectionClient
from crawlee.memory_storage_client.base_resource_collection_client import (
    BaseResourceCollectionClient as BaseMemoryResourceCollectionClient,
)
from crawlee.memory_storage_client.key_value_store_client import KeyValueStoreClient
from crawlee.storages.models import KeyValueStoreListPage


class KeyValueStoreCollectionClient(  # type: ignore
    BaseMemoryResourceCollectionClient,
    BaseKeyValueStoreCollectionClient,
):
    """Subclient for manipulating key-value stores."""

    @property
    @override
    def _client_class(self) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    @override
    def _get_storage_client_cache(self) -> list[KeyValueStoreClient]:
        return self._memory_storage_client.key_value_stores_handled

    @override
    async def list(
        self,
        *,
        unnamed: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool | None = None,
    ) -> KeyValueStoreListPage:
        storage_client_cache = self._get_storage_client_cache()
        items = [storage.resource_info for storage in storage_client_cache]

        return KeyValueStoreListPage(
            total=len(items),
            count=len(items),
            offset=0,
            limit=len(items),
            desc=False,
            items=sorted(items, key=lambda item: item.created_at),
        )
