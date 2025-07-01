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
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.storage_clients.models import KeyValueStoreMetadata

from ._base import Storage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients._base import KeyValueStoreClient
    from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecordMetadata
else:
    from crawlee._utils.recoverable_state import RecoverableState

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

    _autosaved_values: ClassVar[
        dict[
            str,
            dict[str, RecoverableState[AutosavedValue]],
        ]
    ] = {}
    """Cache for recoverable (auto-saved) values."""

    def __init__(self, client: KeyValueStoreClient, id: str, name: str | None) -> None:
        """Initialize a new instance.

        Preferably use the `KeyValueStore.open` constructor to create a new instance.

        Args:
            client: An instance of a storage client.
            id: The unique identifier of the storage.
            name: The name of the storage, if available.
        """
        self._client = client
        self._id = id
        self._name = name

        self._autosave_lock = asyncio.Lock()
        """Lock for autosaving values to prevent concurrent modifications."""

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str | None:
        return self._name

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return await self._client.get_metadata()

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
        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_client = service_locator.get_storage_client() if storage_client is None else storage_client

        return await service_locator.storage_instance_manager.open_storage_instance(
            cls,
            id=id,
            name=name,
            configuration=configuration,
            client_opener=storage_client.create_kvs_client,
        )

    @override
    async def drop(self) -> None:
        storage_instance_manager = service_locator.storage_instance_manager
        storage_instance_manager.remove_from_cache(self)

        await self._clear_cache()  # Clear cache with persistent values.
        await self._client.drop()

    @override
    async def purge(self) -> None:
        await self._client.purge()

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

    async def record_exists(self, key: str) -> bool:
        """Check if a record with the given key exists in the key-value store.

        Args:
            key: Key of the record to check for existence.

        Returns:
            True if a record with the given key exists, False otherwise.
        """
        return await self._client.record_exists(key=key)

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
