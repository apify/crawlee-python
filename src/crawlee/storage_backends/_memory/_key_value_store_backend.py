from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee._utils.raise_if_too_many_kwargs import raise_if_too_many_kwargs
from crawlee.storage_backends._base import KeyValueStoreBackend
from crawlee.storage_backends._memory._utils import json_snapshot
from crawlee.storage_backends.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class MemoryKeyValueStoreBackend(KeyValueStoreBackend):
    """Memory implementation of the key-value store backend.

    This backend stores data in memory as Python dictionaries. No data is persisted between
    process runs, meaning all stored data is lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler operations where
    persistence is not required.

    The memory implementation provides fast access to data but is limited by available memory and
    does not support data sharing across different processes.

    A value with a JSON content type is stored as an independent JSON snapshot, serialized the same way as by the
    file-system backend, and every read returns a fresh copy of it. Other values are stored as they are.
    """

    def __init__(
        self,
        *,
        metadata: KeyValueStoreMetadata,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryKeyValueStoreBackend.open` class method to create a new instance.
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
        alias: str | None,
    ) -> Self:
        """Open or create a new memory key-value store backend.

        This method creates a new in-memory key-value store instance. Unlike persistent storage implementations,
        memory KVS don't check for existing stores with the same name or ID since all data exists only in memory
        and is lost when the process terminates.

        Alias does not have any effect on the memory storage backend implementation, because unnamed storages
        are supported by default, since data are not persisted.

        Args:
            id: The ID of the key-value store. If not provided, a random ID will be generated.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.

        Returns:
            An instance for the opened or created storage backend.

        Raises:
            ValueError: If both name and alias are provided.
        """
        # Validate input parameters.
        raise_if_too_many_kwargs(id=id, name=name, alias=alias)

        # Create a new key-value store
        store_id = id or crypto_random_object_id()
        now = datetime.now(UTC)

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

        record = self._records.get(key, None)
        if record is None:
            return None

        if 'application/json' in record.content_type:
            return record.model_copy(update={'value': json_snapshot(record.value)})

        return record

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        content_type = content_type or infer_mime_type(value)
        size = sys.getsizeof(value)

        if 'application/json' in content_type:
            value = json_snapshot(value)

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

        # Yield metadata for each key, skipping records deleted while the iteration was suspended.
        for key in keys:
            record = self._records.get(key)
            if record is None:
                continue
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
        now = datetime.now(UTC)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now
