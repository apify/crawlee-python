from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from typing_extensions import override

from crawlee import service_locator
from crawlee._utils.docs import docs_group
from crawlee.events._types import Event, EventPersistStateData
from crawlee.storage_clients.models import KeyValueStoreKeyInfo, KeyValueStoreMetadata, StorageMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee._types import JsonSerializable
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient

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

    # Cache for persistent (auto-saved) values
    _general_cache: ClassVar[dict[str, dict[str, dict[str, JsonSerializable]]]] = {}
    _persist_state_event_started = False

    def __init__(self, id: str, name: str | None, storage_client: StorageClient) -> None:
        self._id = id
        self._name = name
        datetime_now = datetime.now(timezone.utc)
        self._storage_object = StorageMetadata(
            id=id, name=name, accessed_at=datetime_now, created_at=datetime_now, modified_at=datetime_now
        )

        # Get resource clients from storage client
        self._resource_client = storage_client.key_value_store(self._id)
        self._autosave_lock = asyncio.Lock()

    @classmethod
    def from_storage_object(cls, storage_client: StorageClient, storage_object: StorageMetadata) -> KeyValueStore:
        """Create a new instance of KeyValueStore from a storage metadata object."""
        key_value_store = KeyValueStore(
            id=storage_object.id,
            name=storage_object.name,
            storage_client=storage_client,
        )

        key_value_store.storage_object = storage_object
        return key_value_store

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @property
    @override
    def storage_object(self) -> StorageMetadata:
        return self._storage_object

    @storage_object.setter
    @override
    def storage_object(self, storage_object: StorageMetadata) -> None:
        self._storage_object = storage_object

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
        storage_client: StorageClient | None = None,
    ) -> KeyValueStore:
        from crawlee.storages._creation_management import open_storage

        configuration = configuration or service_locator.get_configuration()
        storage_client = storage_client or service_locator.get_storage_client()

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
        self._clear_cache()
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

    async def get_public_url(self, key: str) -> str:
        """Get the public URL for the given key.

        Args:
            key: Key of the record for which URL is required.

        Returns:
            The public URL for the given key.
        """
        return await self._resource_client.get_public_url(key)

    async def get_auto_saved_value(
        self,
        key: str,
        default_value: dict[str, JsonSerializable] | None = None,
    ) -> dict[str, JsonSerializable]:
        """Gets a value from KVS that will be automatically saved on changes.

        Args:
            key: Key of the record, to store the value.
            default_value: Value to be used if the record does not exist yet. Should be a dictionary.

        Returns:
            Returns the value of the key.
        """
        default_value = {} if default_value is None else default_value

        async with self._autosave_lock:
            if key in self._cache:
                return self._cache[key]

            value = await self.get_value(key, default_value)

            if not isinstance(value, dict):
                raise TypeError(
                    f'Expected dictionary for persist state value at key "{key}, but got {type(value).__name__}'
                )

            self._cache[key] = value

        self._ensure_persist_event()

        return value

    @property
    def _cache(self) -> dict[str, dict[str, JsonSerializable]]:
        """Cache dictionary for storing auto-saved values indexed by store ID."""
        if self._id not in self._general_cache:
            self._general_cache[self._id] = {}
        return self._general_cache[self._id]

    async def _persist_save(self, _event_data: EventPersistStateData | None = None) -> None:
        """Save cache with persistent values. Can be used in Event Manager."""
        for key, value in self._cache.items():
            await self.set_value(key, value)

    def _ensure_persist_event(self) -> None:
        """Setup persist state event handling if not already done."""
        if self._persist_state_event_started:
            return

        event_manager = service_locator.get_event_manager()
        event_manager.on(event=Event.PERSIST_STATE, listener=self._persist_save)
        self._persist_state_event_started = True

    def _clear_cache(self) -> None:
        """Clear cache with persistent values."""
        self._cache.clear()

    def _drop_persist_state_event(self) -> None:
        """Off event_manager listener and drop event status."""
        if self._persist_state_event_started:
            event_manager = service_locator.get_event_manager()
            event_manager.off(event=Event.PERSIST_STATE, listener=self._persist_save)
        self._persist_state_event_started = False

    async def persist_autosaved_values(self) -> None:
        """Force persistent values to be saved without waiting for an event in Event Manager."""
        if self._persist_state_event_started:
            await self._persist_save()
