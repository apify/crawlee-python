from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, TypeVar, overload

from typing_extensions import override

from crawlee.storages.base_storage import BaseStorage
from crawlee.storages.types import KeyValueStoreRecordInfo

if TYPE_CHECKING:
    from crawlee.configuration import Configuration
    from crawlee.resource_clients import KeyValueStoreClient, KeyValueStoreCollectionClient
    from crawlee.storage_clients import MemoryStorageClient

T = TypeVar('T')


class KeyValueStore(BaseStorage):
    """Represents a key-value based storage for reading data records or files.

    Each record is identified by a unique key and associated with a MIME content type. This class is used within
    crawler runs to store inputs and outputs, typically in JSON format, but supports other types as well.

    The data can be stored on a local filesystem or in the cloud, determined by the `CRAWLEE_LOCAL_STORAGE_DIR`
    environment variable.

    By default, data is stored in `{CRAWLEE_LOCAL_STORAGE_DIR}/key_value_stores/{STORE_ID}/{INDEX}.{EXT}`, where
    `{STORE_ID}` is either "default" or specified by `CRAWLEE_DEFAULT_KEY_VALUE_STORE_ID`, `{KEY}` is the record key,
    and `{EXT}` is the MIME type.

    To open a key-value store, use the class method `open`, providing either an `id_` or `name` along with optional
    `config`. If neither is provided, the default store for the crawler run is used. Opening a non-existent store by
    `id_` raises an error, while a non-existent store by `name` is created.

    Usage:
        kvs = await KeyValueStore.open(id_='my_kvs_id')
    """

    def __init__(
        self,
        id_: str,
        name: str | None,
        configuration: Configuration,
        client: MemoryStorageClient,
    ) -> None:
        super().__init__(id_=id_, name=name, client=client, configuration=configuration)
        self._key_value_store_client = client.key_value_store(self.id)

    # @classmethod
    # @override
    # async def open(
    #     cls,
    #     *,
    #     id_: str | None = None,
    #     name: str | None = None,
    #     configuration: Configuration | None = None,
    # ) -> KeyValueStore:
    #     return await super().open(id_=id_, name=name, configuration=configuration)

    @classmethod
    @override
    def _get_human_friendly_label(cls) -> str:
        return 'Key-value store'

    @classmethod
    @override
    def _get_default_id(cls, configuration: Configuration) -> str:
        return configuration.default_key_value_store_id

    @classmethod
    @override
    def _get_single_storage_client(cls, id_: str, client: MemoryStorageClient) -> KeyValueStoreClient:
        return client.key_value_store(id_)

    @classmethod
    @override
    def _get_storage_collection_client(cls, client: MemoryStorageClient) -> KeyValueStoreCollectionClient:
        return client.key_value_stores()

    @overload
    async def get_value(self, key: str) -> Any: ...

    @overload
    async def get_value(self, key: str, default_value: T) -> T: ...

    @overload
    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def get_value(self, key: str, default_value: T | None = None) -> T | None:
        """Get a value from the key-value store.

        Args:
            key: Key of the record to retrieve.
            default_value: Default value returned in case the record does not exist.

        Returns:
            Any: The value associated with the given key. `default_value` is used in case the record does not exist.
        """
        record = await self._key_value_store_client.get_record(key)
        return record.value if record else default_value

    async def iterate_keys(self, exclusive_start_key: str | None = None) -> AsyncIterator[KeyValueStoreRecordInfo]:
        """Iterate over the keys in the key-value store.

        Args:
            exclusive_start_key: All keys up to this one (including) are skipped from the result.

        Yields:
            An object, where `key` is the record key, and `info` is an object that contains a single property `size`
                indicating size of the record.
        """
        while True:
            list_keys = await self._key_value_store_client.list_keys(exclusive_start_key=exclusive_start_key)
            for item in list_keys.items:
                yield KeyValueStoreRecordInfo(key=item.key, size=item.size)

            if not list_keys.is_truncated:
                break
            exclusive_start_key = list_keys.next_exclusive_start_key

    async def set_value(
        self,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        """Set or delete a value in the key-value store.

        Args:
            key: The key under which the value should be saved.
            value: The value to save. If the value is `None`, the corresponding key-value pair will be deleted.
            content_type: The content type of the saved value.
        """
        if value is None:
            return await self._key_value_store_client.delete_record(key)

        return await self._key_value_store_client.set_record(key, value, content_type)

    async def drop(self) -> None:
        """Remove the key-value store either from the Apify cloud storage or from the local directory."""
        await self._key_value_store_client.delete()
        self._remove_from_cache()
