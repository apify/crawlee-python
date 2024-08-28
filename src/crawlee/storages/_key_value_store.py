from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, TypeVar, overload

from typing_extensions import override

from crawlee.base_storage_client._models import KeyValueStoreKeyInfo, KeyValueStoreMetadata
from crawlee.storages._base_storage import BaseStorage

if TYPE_CHECKING:
    from crawlee.base_storage_client import BaseStorageClient
    from crawlee.configuration import Configuration

T = TypeVar('T')


class KeyValueStore(BaseStorage):
    """Represents a key-value based storage for reading and writing data records or files.

    Each data record is identified by a unique key and associated with a specific MIME content type. This class is
    commonly used in crawler runs to store inputs and outputs, typically in JSON format, but it also supports other
    content types.

    Data can be stored either locally or in the cloud. It depends on the setup of underlying storage client.
    By default a `MemoryStorageClient` is used, but it can be changed to a different one.

    By default, data is stored using the following path structure:
    ```
    {CRAWLEE_STORAGE_DIR}/key_value_stores/{STORE_ID}/{KEY}.{EXT}
    ```
    - `{CRAWLEE_STORAGE_DIR}`: The root directory for all storage data specified by the environment variable.
    - `{STORE_ID}`: The identifier for the key-value store, either "default" or as specified by
      `CRAWLEE_DEFAULT_KEY_VALUE_STORE_ID`.
    - `{KEY}`: The unique key for the record.
    - `{EXT}`: The file extension corresponding to the MIME type of the content.

    To open a key-value store, use the `open` class method, providing an `id`, `name`, or optional `configuration`.
    If none are specified, the default store for the current crawler run is used. Attempting to open a store by `id`
    that does not exist will raise an error; however, if accessed by `name`, the store will be created if it does not
    already exist.

    Usage:
    ```python
    kvs = await KeyValueStore.open(name='my_kvs')
    ```
    """

    def __init__(
        self,
        id: str,
        name: str | None,
        configuration: Configuration,
        client: BaseStorageClient,
    ) -> None:
        self._id = id
        self._name = name
        self._configuration = configuration

        # Get resource clients from storage client
        self._resource_client = client.key_value_store(self._id)

    @override
    @property
    def id(self) -> str:
        return self._id

    @override
    @property
    def name(self) -> str | None:
        return self._name

    async def get_info(self) -> KeyValueStoreMetadata | None:
        """Get an object containing general information about the key value store."""
        return await self._resource_client.get()

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: BaseStorageClient | None = None,
    ) -> KeyValueStore:
        from crawlee.storages._creation_management import open_storage

        return await open_storage(
            storage_class=cls,
            id=id,
            name=name,
            configuration=configuration,
            storage_client=storage_client,
        )

    @override
    async def drop(self) -> None:
        from crawlee.storages._creation_management import remove_storage_from_cache

        await self._resource_client.delete()
        remove_storage_from_cache(storage_class=self.__class__, id=self._id, name=self._name)

    @overload
    async def get_value(self, key: str) -> Any: ...

    @overload
    async def get_value(self, key: str, default_value: T) -> T: ...

    @overload
    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def get_value(self, key: str, default_value: T | None = None) -> T | None:
        """Get a value from the KVS.

        Args:
            key: Key of the record to retrieve.
            default_value: Default value returned in case the record does not exist.

        Returns:
            The value associated with the given key. `default_value` is used in case the record does not exist.
        """
        record = await self._resource_client.get_record(key)
        return record.value if record else default_value

    async def iterate_keys(self, exclusive_start_key: str | None = None) -> AsyncIterator[KeyValueStoreKeyInfo]:
        """Iterate over the existing keys in the KVS.

        Args:
            exclusive_start_key: Key to start the iteration from.

        Yields:
            Information about the key.
        """
        while True:
            list_keys = await self._resource_client.list_keys(exclusive_start_key=exclusive_start_key)
            for item in list_keys.items:
                yield KeyValueStoreKeyInfo(key=item.key, size=item.size)

            if not list_keys.is_truncated:
                break
            exclusive_start_key = list_keys.next_exclusive_start_key

    async def set_value(
        self,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        """Set a value in the KVS.

        Args:
            key: Key of the record to set.
            value: Value to set. If `None`, the record is deleted.
            content_type: Content type of the record.
        """
        if value is None:
            return await self._resource_client.delete_record(key)

        return await self._resource_client.set_record(key, value, content_type)
