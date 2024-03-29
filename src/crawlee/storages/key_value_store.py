from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, NamedTuple, TypedDict, TypeVar, cast, overload

from crawlee.storages.base_storage import BaseStorage

if TYPE_CHECKING:
    from crawlee.config import Config
    from crawlee.memory_storage import MemoryStorageClient
    from crawlee.memory_storage.resource_clients import KeyValueStoreClient, KeyValueStoreCollectionClient

T = TypeVar('T')


class IterateKeysInfo(TypedDict):
    """Contains information about a key-value store record."""

    size: int


class IterateKeysTuple(NamedTuple):
    """A tuple representing a key-value store record."""

    key: str
    info: IterateKeysInfo


class KeyValueStore(BaseStorage):
    """The `KeyValueStore` class represents a key-value store.

    You can imagine it as a simple data storage that is used for saving and reading data records or files. Each data
    record is represented by a unique key and associated with a MIME content type.

    Each crawler run is associated with a default key-value store, which is created exclusively for the run.
    By convention, the crawler input and output are stored into the default key-value store under the `INPUT`
    and `OUTPUT` key, respectively. Typically, input and output are JSON files, although it can be any other
    format.

    If the `CRAWLEE_LOCAL_STORAGE_DIR` environment variable is set, the data is stored in
    the local directory in the following files:
    ```
    {CRAWLEE_LOCAL_STORAGE_DIR}/key_value_stores/{STORE_ID}/{INDEX}.{EXT}
    ```

    Note that `{STORE_ID}` is the name or ID of the key-value store. The default key-value store has ID: `default`,
    unless you override it by setting the `CRAWLEE_DEFAULT_KEY_VALUE_STORE_ID` environment variable.
    The `{KEY}` is the key of the record and `{EXT}` corresponds to the MIME content type of the data value.
    """

    def __init__(
        self,
        id_: str,
        name: str | None,
        config: Config,
        client: MemoryStorageClient,
    ) -> None:
        """Create a new instance.

        Args:
            id_: ID of the key-value store.
            name: Name of the key-value store.
            config: The configuration.
            client: The storage client which should be used.
        """
        super().__init__(id_=id_, name=name, client=client, config=config)
        self._key_value_store_client = client.key_value_store(self.id)

    @classmethod
    async def open(
        cls,
        *,
        id_: str | None = None,
        name: str | None = None,
        config: Config | None = None,
    ) -> KeyValueStore:
        """Open a key-value store.

        Key-value stores are used to store records or files, along with their MIME content type.
        The records are stored and retrieved using a unique key.
        The actual data is stored either on a local filesystem or in the Apify cloud.

        Args:
            id_: ID of the key-value store to be opened. If neither `id` nor `name` are provided, the method returns
                the default key-value store associated with the actor run. If the key-value store with the given
                ID does not exist, it raises an error.
            name: Name of the key-value store to be opened. If neither `id` nor `name` are provided, the method returns
                the default key-value store associated with the actor run. If the key-value store with the given name
                does not exist, it is created.
            config: Configuration settings.

        Returns:
            KeyValueStore: An instance of the `KeyValueStore` class for the given ID or name.
        """
        storage = await super().open(id_=id_, name=name, config=config)
        return cast(KeyValueStore, storage)

    @classmethod
    def _get_human_friendly_label(cls) -> str:
        return 'Key-value store'

    @classmethod
    def _get_default_id(cls, config: Config) -> str:
        return config.default_key_value_store_id

    @classmethod
    def _get_single_storage_client(cls, id_: str, client: MemoryStorageClient) -> KeyValueStoreClient:
        return client.key_value_store(id_)

    @classmethod
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
        return record['value'] if record else default_value

    async def iterate_keys(self, exclusive_start_key: str | None = None) -> AsyncIterator[IterateKeysTuple]:
        """Iterate over the keys in the key-value store.

        Args:
            exclusive_start_key: All keys up to this one (including) are skipped from the result.

        Yields:
            IterateKeysTuple: A tuple `(key, info)`, where `key` is the record key, and `info` is an object that
                contains a single property `size` indicating size of the record in bytes.
        """
        while True:
            list_keys = await self._key_value_store_client.list_keys(exclusive_start_key=exclusive_start_key)
            for item in list_keys['items']:
                yield IterateKeysTuple(item['key'], {'size': item['size']})

            if not list_keys['isTruncated']:
                break
            exclusive_start_key = list_keys['nextExclusiveStartKey']

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
