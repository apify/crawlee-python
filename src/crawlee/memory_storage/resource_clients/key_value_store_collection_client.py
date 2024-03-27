from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.memory_storage.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.memory_storage.resource_clients.key_value_store_client import KeyValueStoreClient

if TYPE_CHECKING:
    from crawlee._utils.types import ListPage


class KeyValueStoreCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating key-value stores."""

    def _get_storage_client_cache(self) -> list[KeyValueStoreClient]:
        return self._memory_storage_client._key_value_stores_handled

    def _get_resource_client_class(self) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    async def list(self) -> ListPage:
        """List the available key-value stores.

        Returns:
            ListPage: The list of available key-value stores matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        _id: str | None = None,
    ) -> dict:
        """Retrieve a named key-value store, or create a new one when it doesn't exist.

        Args:
            name (str, optional): The name of the key-value store to retrieve or create.
            schema (Dict, optional): The schema of the key-value store

        Returns:
            dict: The retrieved or newly-created key-value store.
        """
        return await super().get_or_create(name=name, schema=schema, _id=_id)
