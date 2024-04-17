from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.resource_clients.key_value_store_client import KeyValueStoreClient
from crawlee.storages.models import KeyValueStoresListPage

if TYPE_CHECKING:
    from crawlee.storages.models import BaseStorageMetadata


class KeyValueStoreCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating key-value stores."""

    @property
    @override
    def _client_class(self) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    @override
    def _get_storage_client_cache(self) -> list[KeyValueStoreClient]:
        return self._memory_storage_client.key_value_stores_handled

    @override
    async def list(self) -> KeyValueStoresListPage:
        storage_client_cache = self._get_storage_client_cache()
        items = [storage.resource_info for storage in storage_client_cache]

        return KeyValueStoresListPage(
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
        """Retrieve a named key-value store, or create a new one when it doesn't exist.

        Args:
            name: The name of the key-value store to retrieve or create.
            schema: The schema of the key-value store
            id: The id of the key-value store to retrieve or create.

        Returns:
            The retrieved or newly-created key-value store.
        """
        return await super().get_or_create(name=name, schema=schema, id=id)
