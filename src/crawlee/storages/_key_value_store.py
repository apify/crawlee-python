from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import KeyValueStoreClient
    from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecordMetadata

T = TypeVar('T')


@docs_group('Classes')
class KeyValueStore(Storage):
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

    ### Usage

    ```python
    from crawlee.storages import KeyValueStore

    kvs = await KeyValueStore.open(name='my_kvs')
    ```
    """

    _cache_by_id: ClassVar[dict[str, KeyValueStore]] = {}
    """A dictionary to cache key-value stores by their IDs."""

    _cache_by_name: ClassVar[dict[str, KeyValueStore]] = {}
    """A dictionary to cache key-value stores by their names."""

    def __init__(self, client: KeyValueStoreClient) -> None:
        """Initialize a new instance.

        Preferably use the `KeyValueStore.open` constructor to create a new instance.

        Args:
            client: An instance of a key-value store client.
        """
        self._client = client

    @override
    @property
    def id(self) -> str:
        return self._client.metadata.id

    @override
    @property
    def name(self) -> str | None:
        return self._client.metadata.name

    @override
    @property
    def metadata(self) -> KeyValueStoreMetadata:
        return self._client.metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> KeyValueStore:
        if id and name:
            raise ValueError('Only one of "id" or "name" can be specified, not both.')

        # Check if key value store is already cached by id or name
        if id and id in cls._cache_by_id:
            return cls._cache_by_id[id]
        if name and name in cls._cache_by_name:
            return cls._cache_by_name[name]

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        client = await storage_client.open_key_value_store_client(
            id=id,
            name=name,
            configuration=configuration,
        )

        kvs = cls(client)

        # Cache the key value store by id and name if available
        if kvs.id:
            cls._cache_by_id[kvs.id] = kvs
        if kvs.name:
            cls._cache_by_name[kvs.name] = kvs

        return kvs

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        if self.id in self._cache_by_id:
            del self._cache_by_id[self.id]
        if self.name and self.name in self._cache_by_name:
            del self._cache_by_name[self.name]

        await self._client.drop()

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
        record = await self._client.get_value(key=key)
        return record.value if record else default_value

    async def set_value(
        self,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        """Set a value in the KVS.

        Args:
            key: Key of the record to set.
            value: Value to set.
            content_type: The MIME content type string.
        """
        await self._client.set_value(key=key, value=value, content_type=content_type)

    async def delete_value(self, key: str) -> None:
        """Delete a value from the KVS.

        Args:
            key: Key of the record to delete.
        """
        await self._client.delete_value(key=key)

    async def iterate_keys(
        self,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        """Iterate over the existing keys in the KVS.

        Args:
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of keys to return. None means no limit.

        Yields:
            Information about the key.
        """
        async for item in self._client.iterate_keys(
            exclusive_start_key=exclusive_start_key,
            limit=limit,
        ):
            yield item

    async def list_keys(
        self,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> list[KeyValueStoreRecordMetadata]:
        """List all the existing keys in the KVS.

        It uses client's `iterate_keys` method to get the keys.

        Args:
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of keys to return.

        Returns:
            A list of keys in the KVS.
        """
        return [
            key
            async for key in self._client.iterate_keys(
                exclusive_start_key=exclusive_start_key,
                limit=limit,
            )
        ]

    async def get_public_url(self, key: str) -> str:
        """Get the public URL for the given key.

        Args:
            key: Key of the record for which URL is required.

        Returns:
            The public URL for the given key.
        """
        return await self._client.get_public_url(key=key)
