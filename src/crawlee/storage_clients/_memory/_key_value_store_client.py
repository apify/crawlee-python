from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class MemoryKeyValueStoreClient(KeyValueStoreClient):
    """Memory implementation of the key-value store client.

    This client stores data in memory as Python dictionaries. No data is persisted between
    process runs, meaning all stored data is lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler operations where
    persistence is not required.

    The memory implementation provides fast access to data but is limited by available memory and
    does not support data sharing across different processes.
    """

    def __init__(
        self,
        *,
        metadata: KeyValueStoreMetadata,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryKeyValueStoreClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._records = dict[str, KeyValueStoreRecord]()
        """Dictionary to hold key-value records."""

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
    ) -> MemoryKeyValueStoreClient:
        """Open or create a new memory key-value store client.

        This method creates a new in-memory key-value store instance. Unlike persistent storage implementations,
        memory KVS don't check for existing stores with the same name or ID since all data exists only in memory
        and is lost when the process terminates.

        Args:
            id: The ID of the key-value store. If not provided, a random ID will be generated.
            name: The name of the key-value store. If not provided, the store will be unnamed.

        Returns:
            An instance for the opened or created storage client.
        """
        # Otherwise create a new key-value store
        store_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        metadata = KeyValueStoreMetadata(
            id=store_id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
        )

        return cls(metadata=metadata)

    @override
    async def drop(self) -> None:
        self._records.clear()
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def purge(self) -> None:
        self._records.clear()
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        await self._update_metadata(update_accessed_at=True)

        # Return None if key doesn't exist
        return self._records.get(key, None)

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        content_type = content_type or infer_mime_type(value)
        size = sys.getsizeof(value)

        # Create and store the record
        record = KeyValueStoreRecord(
            key=key,
            value=value,
            content_type=content_type,
            size=size,
        )

        self._records[key] = record

        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def delete_value(self, *, key: str) -> None:
        if key in self._records:
            del self._records[key]
            await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        await self._update_metadata(update_accessed_at=True)

        # Get all keys, sorted alphabetically
        keys = sorted(self._records.keys())

        # Apply exclusive_start_key filter if provided
        if exclusive_start_key is not None:
            keys = [k for k in keys if k > exclusive_start_key]

        # Apply limit if provided
        if limit is not None:
            keys = keys[:limit]

        # Yield metadata for each key
        for key in keys:
            record = self._records[key]
            yield KeyValueStoreRecordMetadata(
                key=key,
                content_type=record.content_type,
                size=record.size,
            )

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for memory key-value stores.')

    @override
    async def record_exists(self, *, key: str) -> bool:
        await self._update_metadata(update_accessed_at=True)
        return key in self._records

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the key-value store metadata with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now
