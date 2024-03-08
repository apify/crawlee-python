from __future__ import annotations

from typing import TYPE_CHECKING

from apify_shared.utils import ignore_docs

from apify._memory_storage.resource_clients.base_resource_collection_client import BaseResourceCollectionClient
from apify._memory_storage.resource_clients.key_value_store import KeyValueStoreClient

if TYPE_CHECKING:
    from apify_shared.models import ListPage


@ignore_docs
class KeyValueStoreCollectionClient(BaseResourceCollectionClient):
    """Sub-client for manipulating key-value stores."""

    def _get_storage_client_cache(self: KeyValueStoreCollectionClient) -> list[KeyValueStoreClient]:
        return self._memory_storage_client._key_value_stores_handled

    def _get_resource_client_class(self: KeyValueStoreCollectionClient) -> type[KeyValueStoreClient]:
        return KeyValueStoreClient

    async def list(self: KeyValueStoreCollectionClient) -> ListPage:
        """List the available key-value stores.

        Returns:
            ListPage: The list of available key-value stores matching the specified filters.
        """
        return await super().list()

    async def get_or_create(
        self: KeyValueStoreCollectionClient,
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
