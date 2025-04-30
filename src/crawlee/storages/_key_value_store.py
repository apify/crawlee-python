from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from pydantic import RootModel
from typing_extensions import override

from crawlee import service_locator
from crawlee._types import JsonSerializable  # noqa: TC001
from crawlee._utils.docs import docs_group
from crawlee.storage_clients.models import KeyValueStoreMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee._utils.recoverable_state import RecoverableState
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import KeyValueStoreClient
    from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecordMetadata

T = TypeVar('T')

logger = getLogger(__name__)


class AutosavedValue(RootModel):
    root: dict[str, JsonSerializable]


@docs_group('Classes')
class KeyValueStore(Storage):
    """Key-value store is a storage for reading and writing data records with unique key identifiers.

    The key-value store class acts as a high-level interface for storing, retrieving, and managing data records
    identified by unique string keys. It abstracts away the underlying storage implementation details,
    allowing you to work with the same API regardless of whether data is stored in memory, on disk,
    or in the cloud.

    Each data record is associated with a specific MIME content type, allowing storage of various
    data formats such as JSON, text, images, HTML snapshots or any binary data. This class is
    commonly used to store inputs, outputs, and other artifacts of crawler operations.

    You can instantiate a key-value store using the `open` class method, which will create a store
    with the specified name or id. The underlying storage implementation is determined by the configured
    storage client.

    ### Usage

    ```python
    from crawlee.storages import KeyValueStore

    # Open a named key-value store
    kvs = await KeyValueStore.open(name='my-store')

    # Store and retrieve data
    await kvs.set_value('product-1234.json', [{'name': 'Smartphone', 'price': 799.99}])
    product = await kvs.get_value('product-1234')
    ```
    """

    _cache: ClassVar[dict[str, KeyValueStore]] = {}
    """A dictionary to cache key-value stores."""

    # Cache for recoverable (auto-saved) values
    _autosaved_values: ClassVar[
        dict[
            str,
            dict[str, RecoverableState[AutosavedValue]],
        ]
    ] = {}

    def __init__(self, client: KeyValueStoreClient, cache_key: str) -> None:
        """Initialize a new instance.

        Preferably use the `KeyValueStore.open` constructor to create a new instance.

        Args:
            client: An instance of a key-value store client.
            cache_key: A unique key to identify the key-value store in the cache.
        """
        self._client = client
        self._cache_key = cache_key
        self._autosave_lock = asyncio.Lock()
        self._persist_state_event_started = False

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

        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        cache_key = cls.compute_cache_key(
            id=id,
            name=name,
            configuration=configuration,
            storage_client=storage_client,
        )

        if cache_key in cls._cache:
            return cls._cache[cache_key]

        client = await storage_client.open_key_value_store_client(
            id=id,
            name=name,
            configuration=configuration,
        )

        kvs = cls(client, cache_key)
        cls._cache[cache_key] = kvs
        return kvs

    @override
    async def drop(self) -> None:
        # Remove from cache before dropping
        if self._cache_key in self._cache:
            del self._cache[self._cache_key]

        # Clear cache with persistent values
        await self._clear_cache()

        # Drop the key-value store client
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

    async def get_auto_saved_value(
        self,
        key: str,
        default_value: dict[str, JsonSerializable] | None = None,
    ) -> dict[str, JsonSerializable]:
        """Get a value from KVS that will be automatically saved on changes.

        Args:
            key: Key of the record, to store the value.
            default_value: Value to be used if the record does not exist yet. Should be a dictionary.

        Returns:
            Return the value of the key.
        """
        from crawlee._utils.recoverable_state import RecoverableState

        default_value = {} if default_value is None else default_value

        async with self._autosave_lock:
            cache = self._autosaved_values.setdefault(self.id, {})

            if key in cache:
                return cache[key].current_value.root

            cache[key] = recoverable_state = RecoverableState(
                default_state=AutosavedValue(default_value),
                persistence_enabled=True,
                persist_state_kvs_id=self.id,
                persist_state_key=key,
                logger=logger,
            )

            await recoverable_state.initialize()

        return recoverable_state.current_value.root

    async def persist_autosaved_values(self) -> None:
        """Force autosaved values to be saved without waiting for an event in Event Manager."""
        if self.id in self._autosaved_values:
            cache = self._autosaved_values[self.id]
            for value in cache.values():
                await value.persist_state()

    async def _clear_cache(self) -> None:
        """Clear cache with autosaved values."""
        if self.id in self._autosaved_values:
            cache = self._autosaved_values[self.id]
            for value in cache.values():
                await value.teardown()
            cache.clear()
