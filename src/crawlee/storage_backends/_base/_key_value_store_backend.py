from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee.storage_backends.models import KeyValueStoreRecord

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from crawlee.storage_backends.models import KeyValueStoreKeysListPage, KeyValueStoreMetadata


class KeyValueStoreBackend(ABC):
    """An abstract class for key-value store (KVS) storage backends.

    Key-value store backends provide an interface for accessing and manipulating KVS storage. They handle
    operations like getting, setting, deleting KVS values across different storage backends.

    These backends are specific to the type of storage they manage (`Dataset`, `KeyValueStore`,
    `RequestQueue`), and can operate with various storage systems including memory, file system,
    databases, and cloud storage solutions.

    A backend stores and returns the record values as raw bytes together with their content type. `KeyValueStore`
    serializes the values before writing them and parses them after reading them.

    This abstract class defines the interface that all specific KVS backends must implement.
    """

    @abstractmethod
    async def get_metadata(self) -> KeyValueStoreMetadata:
        """Get the metadata of the key-value store."""

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole key-value store and remove all its values.

        The backend method for the `KeyValueStore.drop` call.
        """

    @abstractmethod
    async def purge(self) -> None:
        """Purge all items from the key-value store.

        The backend method for the `KeyValueStore.purge` call.
        """

    @abstractmethod
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        """Retrieve the given record from the key-value store, with its value as stored.

        The backend method for the `KeyValueStore.get_value` and `KeyValueStore.get_record` calls.
        """

    async def get_values(self, *, keys: Sequence[str]) -> Mapping[str, KeyValueStoreRecord]:
        """Retrieve the given records from the key-value store, with their values as stored.

        The backend method for the `KeyValueStore.iterate_entries` and `KeyValueStore.iterate_values` calls.

        The default implementation reads the records one by one with `get_value`. Backends that can read several
        records in a single call should override this method, and return the same records as `get_value` does.

        Returns:
            The record of each key that has one. Keys without a record are left out.
        """
        records = dict[str, KeyValueStoreRecord]()
        for key in keys:
            record = await self.get_value(key=key)
            if record is not None:
                records[key] = record
        return records

    @abstractmethod
    async def set_value(self, *, key: str, value: bytes, content_type: str) -> None:
        """Set a value in the key-value store by its key.

        The backend method for the `KeyValueStore.set_value` call.
        """

    @abstractmethod
    async def delete_value(self, *, key: str) -> None:
        """Delete a value from the key-value store by its key.

        The backend method for the `KeyValueStore.delete_value` call, and for `KeyValueStore.set_value` with a `None`
        value.
        """

    @abstractmethod
    async def list_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreKeysListPage:
        """List a single page of the existing keys in the key-value store, ordered by key.

        The backend method for the `KeyValueStore.iterate_keys`, `KeyValueStore.list_keys`,
        `KeyValueStore.iterate_values` and `KeyValueStore.iterate_entries` calls.

        The keys follow an order that stays the same between calls, and `exclusive_start_key` is compared in that
        order. A page may hold fewer than `limit` keys, or none, even when more keys follow, so the caller continues
        from `next_exclusive_start_key` until it's `None`. That key doesn't have to be one of the keys on the page.

        Args:
            prefix: List only the keys that start with this prefix.
            exclusive_start_key: List only the keys that follow this key.
            limit: Maximum number of keys on the page, at least 1.
        """

    @abstractmethod
    async def get_public_url(self, *, key: str) -> str:
        """Get the public URL for the given key.

        The backend method for the `KeyValueStore.get_public_url` call.
        """

    @abstractmethod
    async def record_exists(self, *, key: str) -> bool:
        """Check if a record with the given key exists in the key-value store.

        The backend method for the `KeyValueStore.record_exists` call.

        Args:
            key: The key to check for existence.

        Returns:
            True if a record with the given key exists, False otherwise.
        """
