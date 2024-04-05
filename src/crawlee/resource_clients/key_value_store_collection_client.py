from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from crawlee.resource_clients.key_value_store_client import KeyValueStoreClient

if TYPE_CHECKING:
    from crawlee.storages.types import BaseResourceInfo, ListPage


class KeyValueStoreCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating key-value stores."""

    @property
    @override
    def _client_class(self) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    @override
    def _get_storage_client_cache(self) -> list[KeyValueStoreClient]:
        return self._memory_storage_client.key_value_stores_handled

    async def list(self) -> ListPage:
        """List the available key-value stores.

        Returns:
            The list of available key-value stores matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self,
        *,
        name: str | None = None,
        schema: dict | None = None,
        id_: str | None = None,
    ) -> BaseResourceInfo:
        """Retrieve a named key-value store, or create a new one when it doesn't exist.

        Args:
            name: The name of the key-value store to retrieve or create.
            schema: The schema of the key-value store
            id_: The id of the key-value store to retrieve or create.

        Returns:
            The retrieved or newly-created key-value store.
        """
        return await super().get_or_create(name=name, schema=schema, id_=id_)
