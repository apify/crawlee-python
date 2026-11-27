from __future__ import annotations

import asyncio
from collections.abc import MutableMapping  # noqa: TC003
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from pydantic import RootModel
from typing_extensions import override

from crawlee import service_locator
from crawlee._types import JsonSerializable  # noqa: TC001
from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.storage_backends._utils import batch_records_by_size
from crawlee.storage_backends.models import KeyValueStoreMetadata

from ._base import Storage
from ._utils import parse_value, serialize_value, validate_storage_name

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from crawlee.configuration import Configuration
    from crawlee.storage_backends import StorageBackend
    from crawlee.storage_backends._base import KeyValueStoreBackend
    from crawlee.storage_backends.models import (
        KeyValueStoreMetadata,
        KeyValueStoreRecord,
        KeyValueStoreRecordMetadata,
    )
else:
    from crawlee._utils.recoverable_state import RecoverableState

T = TypeVar('T')

logger = getLogger(__name__)


class AutosavedValue(RootModel):
    root: MutableMapping[str, JsonSerializable]


@docs_group('Storages')
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
    storage backend.

    ### Usage

    ```python
    from crawlee.storages import KeyValueStore

    # Open a named key-value store
    kvs = await KeyValueStore.open(name='my-store')

    # Store and retrieve data
    await kvs.set_value('product-1234', [{'name': 'Smartphone', 'price': 799.99}])
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

    _ITERATE_ENTRIES_BATCH_MAX_KEYS = 100
    """Maximum number of records read with a single storage backend call in `iterate_entries`."""

    _ITERATE_ENTRIES_BATCH_MAX_BYTES = 8 * 1024 * 1024
    """Maximum total size of the records read with a single storage backend call in `iterate_entries`.

    A single record larger than this is still read, but alone in its batch.
    """

    def __init__(self, backend: KeyValueStoreBackend, id: str, name: str | None) -> None:
        """Initialize a new instance.

        Preferably use the `KeyValueStore.open` constructor to create a new instance.

        Args:
            backend: The key-value store backend to use.
            id: The unique identifier of the storage.
            name: The name of the storage, if available.
        """
        validate_storage_name(name)

        self._backend = backend
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
        return await self._backend.get_metadata()

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
        storage_backend: StorageBackend | None = None,
    ) -> KeyValueStore:
        configuration = service_locator.get_configuration() if configuration is None else configuration
        storage_backend = service_locator.get_storage_backend() if storage_backend is None else storage_backend

        backend_opener_coro = storage_backend.create_kvs_backend(
            id=id, name=name, alias=alias, configuration=configuration
        )
        additional_cache_key = storage_backend.get_storage_backend_cache_key(configuration=configuration)

        return await service_locator.storage_instance_manager.open_storage_instance(
            cls,
            id=id,
            name=name,
            alias=alias,
            backend_opener_coro=backend_opener_coro,
            storage_backend_cache_key=additional_cache_key,
        )

    @override
    async def drop(self) -> None:
        await self._clear_store_and_cache(self._backend.drop)

        storage_instance_manager = service_locator.storage_instance_manager
        storage_instance_manager.remove_from_cache(self)

    @override
    async def purge(self) -> None:
        await self._clear_store_and_cache(self._backend.purge)

    @overload
    async def get_value(self, key: str) -> Any: ...

    @overload
    async def get_value(self, key: str, default_value: T) -> T: ...

    @overload
    async def get_value(self, key: str, default_value: T | None = None) -> T | None: ...

    async def get_value(self, key: str, default_value: T | None = None) -> T | None:
        """Get a value from the KVS.

        The value is parsed according to the content type of the record. JSON is parsed, text and XML are returned as
        `str`, and a value with any other content type is returned as `bytes`.

        Args:
            key: Key of the record to retrieve.
            default_value: Default value returned in case the record does not exist.

        Returns:
            The value associated with the given key. `default_value` is used in case the record does not exist.

        Raises:
            ValueError: If the stored value has a JSON content type but isn't valid JSON.
        """
        record = await self._backend.get_value(key=key)
        if record is None:
            return default_value

        return self._parse_record(record)

    async def get_record(self, key: str) -> KeyValueStoreRecord | None:
        """Get a record from the KVS without parsing its value.

        Args:
            key: Key of the record to retrieve.

        Returns:
            The record with its value as the stored bytes and its content type, or `None` if the record does not exist.
        """
        return await self._backend.get_value(key=key)

    async def set_value(
        self,
        key: str,
        value: Any,
        *,
        content_type: str | None = None,
    ) -> None:
        """Set a value in the KVS.

        Args:
            key: Key of the record to set.
            value: Value to set. `bytes`, `bytearray`, `memoryview` and `str` values are stored as they are, and any
                other value is stored as JSON. Objects JSON doesn't support, such as a `datetime` or a Pydantic
                model, are converted with `pydantic_core.to_jsonable_python`. `None` deletes the record.
            content_type: The MIME content type string. If not provided, it's `application/octet-stream` for bytes,
                `text/plain` for `str` and `application/json` for other values.

        Raises:
            TypeError: If `value` is neither `str` nor bytes, and `content_type` isn't JSON, or if JSON can't represent
                it, for example a `dict` with `tuple` keys or an object of a type Pydantic can't serialize.
            ValueError: If `value` is stored as JSON and JSON can't serialize it, for example because of a circular
                reference.
            LookupError: If `value` isn't bytes, and the charset of `content_type` is unknown.
            UnicodeEncodeError: If `value` isn't bytes, and the charset of `content_type` can't encode it.
        """
        if value is None:
            await self._backend.delete_value(key=key)
            return

        value_bytes, content_type = await serialize_value(value, content_type)
        await self._backend.set_value(key=key, value=value_bytes, content_type=content_type)

    async def delete_value(self, key: str) -> None:
        """Delete a value from the KVS.

        Args:
            key: Key of the record to delete.
        """
        await self._backend.delete_value(key=key)

    async def iterate_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        """Iterate over the existing keys in the KVS.

        The keys are listed page by page, so a key set while the iteration is in progress may be yielded if it follows
        the keys listed so far.

        Args:
            prefix: Iterate only over the keys that start with this prefix.
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of keys to return. None means no limit.
            chunk_size: Maximum number of keys listed with a single storage backend call.

        Raises:
            ValueError: If `chunk_size` is less than 1.

        Yields:
            Information about the key.
        """
        async for page in self._iterate_key_pages(
            prefix=prefix,
            exclusive_start_key=exclusive_start_key,
            limit=limit,
            chunk_size=chunk_size,
        ):
            for item in page:
                yield item

    async def iterate_values(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[Any]:
        """Iterate over the values of the existing records in the KVS.

        The records are fetched lazily as the iteration advances, in batches bounded by the record count and by the
        total record size, so only a bounded number of values is held in memory at a time. A record set while the
        iteration is in progress may be yielded if its key follows the keys listed so far.

        Args:
            prefix: Iterate only over the records whose key starts with this prefix.
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of records to iterate over. None means no limit.
            chunk_size: Maximum number of keys listed with a single storage backend call. The values are read in
                separate batches, bounded by the record count and size.

        Raises:
            ValueError: If a stored value has a JSON content type but isn't valid JSON, or if `chunk_size` is less
                than 1.

        Yields:
            The value of each record.
        """
        async for _, value in self.iterate_entries(
            prefix=prefix,
            exclusive_start_key=exclusive_start_key,
            limit=limit,
            chunk_size=chunk_size,
        ):
            yield value

    async def iterate_entries(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[tuple[str, Any]]:
        """Iterate over the existing records in the KVS as `(key, value)` pairs.

        The records are fetched lazily as the iteration advances, in batches bounded by the record count and by the
        total record size, so only a bounded number of values is held in memory at a time. A record deleted while
        the iteration is in progress may or may not be yielded, depending on whether its value was already read. A
        record set while it's in progress may be yielded if its key follows the keys listed so far.

        Args:
            prefix: Iterate only over the records whose key starts with this prefix.
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of records to iterate over. None means no limit.
            chunk_size: Maximum number of keys listed with a single storage backend call. The values are read in
                separate batches, bounded by the record count and size.

        Raises:
            ValueError: If a stored value has a JSON content type but isn't valid JSON, or if `chunk_size` is less
                than 1.

        Yields:
            A `(key, value)` tuple for each record.
        """
        async for page in self._iterate_key_pages(
            prefix=prefix,
            exclusive_start_key=exclusive_start_key,
            limit=limit,
            chunk_size=chunk_size,
        ):
            batches = batch_records_by_size(
                page,
                max_records=self._ITERATE_ENTRIES_BATCH_MAX_KEYS,
                max_bytes=self._ITERATE_ENTRIES_BATCH_MAX_BYTES,
            )
            for batch in batches:
                keys = [item.key for item in batch]
                records = await self._backend.get_values(keys=keys)
                for key in keys:
                    record = records.get(key)
                    if record is not None:
                        yield key, self._parse_record(record)

    def __aiter__(self) -> AsyncIterator[str]:
        """Iterate over all keys in the KVS.

        Allows using the key-value store directly in an `async for` loop, which yields the keys like iterating
        over a `dict` does. Use `iterate_keys` for the key metadata, `iterate_values` for the values, or
        `iterate_entries` for `(key, value)` pairs.

        ### Usage

        ```python
        async for key in kvs:
            print(key)
        ```
        """
        return (metadata.key async for metadata in self.iterate_keys())

    async def list_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> list[KeyValueStoreRecordMetadata]:
        """List the existing keys in the KVS, up to `limit` of them.

        Args:
            prefix: List only the keys that start with this prefix.
            exclusive_start_key: Key to start the iteration from.
            limit: Maximum number of keys to return.

        Returns:
            A list of keys in the KVS.
        """
        return [
            key
            async for key in self.iterate_keys(
                prefix=prefix,
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
        return await self._backend.record_exists(key=key)

    async def get_public_url(self, key: str) -> str:
        """Get the public URL for the given key.

        Args:
            key: Key of the record for which URL is required.

        Returns:
            The public URL for the given key.
        """
        return await self._backend.get_public_url(key=key)

    async def get_auto_saved_value(
        self,
        key: str,
        default_value: MutableMapping[str, JsonSerializable] | None = None,
    ) -> MutableMapping[str, JsonSerializable]:
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
                # Retries the load if an earlier call failed or was cancelled during it.
                cached_state = await cache[key].initialize()
                return cached_state.root

            async def kvs_factory() -> KeyValueStore:
                return self

            cache[key] = recoverable_state = RecoverableState(
                default_state=AutosavedValue(default_value),
                persist_state_key=key,
                persistence_enabled=True,
                persist_state_kvs_factory=kvs_factory,
                logger=logger,
            )

            await recoverable_state.initialize()

        return recoverable_state.current_value.root

    async def persist_autosaved_values(self) -> None:
        """Force autosaved values to be saved without waiting for an event in Event Manager."""
        if self.id in self._autosaved_values:
            async with self._autosave_lock:
                cache = self._autosaved_values[self.id]
                for value in cache.values():
                    await value.persist_state()

    async def _iterate_key_pages(
        self,
        *,
        prefix: str | None,
        exclusive_start_key: str | None,
        limit: int | None,
        chunk_size: int,
    ) -> AsyncIterator[list[KeyValueStoreRecordMetadata]]:
        """Iterate over the pages of keys listed by the storage backend, up to `limit` keys in total."""
        if chunk_size < 1:
            raise ValueError(f'`chunk_size` must be at least 1, got {chunk_size}.')

        remaining = limit

        while remaining is None or remaining > 0:
            page_limit = chunk_size
            if remaining is not None:
                page_limit = min(page_limit, remaining)

            page = await self._backend.list_keys(
                prefix=prefix,
                exclusive_start_key=exclusive_start_key,
                limit=page_limit,
            )
            yield page.items

            if page.next_exclusive_start_key is None:
                return

            exclusive_start_key = page.next_exclusive_start_key
            if remaining is not None:
                remaining -= len(page.items)

    @staticmethod
    def _parse_record(record: KeyValueStoreRecord) -> Any:
        try:
            return parse_value(record.value, record.content_type)
        except ValueError as exc:
            exc.add_note(
                f"The record under key '{record.key}' can't be parsed as '{record.content_type}'. Use "
                '`KeyValueStore.get_record` to read its raw value.'
            )
            raise

    async def _clear_store_and_cache(self, clear_method: Callable[[], Awaitable[None]]) -> None:
        """Clear the store with the backend's `clear_method`, and the cache of autosaved values with it."""
        # The autosaved values go with the records, or their next save would write them back. The lock is held until
        # the records are gone, so that a concurrent `get_auto_saved_value` can't load one of them.
        async with self._autosave_lock:
            cache = self._autosaved_values.get(self.id, {})
            initialized_values = [value for value in cache.values() if value.is_initialized]

            try:
                for value in initialized_values:
                    await value.teardown()

                await clear_method()
            except (Exception, asyncio.CancelledError):
                # A failed call keeps the values, so they listen for `PERSIST_STATE` events again.
                for value in initialized_values:
                    await value.initialize()
                raise

            cache.clear()
