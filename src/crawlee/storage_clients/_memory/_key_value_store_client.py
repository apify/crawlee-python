from __future__ import annotations

import sys
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class MemoryKeyValueStoreClient(KeyValueStoreClient):
    """A memory implementation of the key-value store client.

    This client stores key-value store pairs in memory using a dictionary. No data is persisted,
    which means all data is lost when the process terminates. This implementation is mainly useful
    for testing and development purposes where persistence is not required.
    """

    _cache_by_name: ClassVar[dict[str, MemoryKeyValueStoreClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryKeyValueStoreClient.open` class method to create a new instance.
        """
        self._metadata = KeyValueStoreMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
        )

        # Dictionary to hold key-value records with metadata
        self._records = dict[str, KeyValueStoreRecord]()

    @override
    @property
    def metadata(self) -> KeyValueStoreMetadata:
        return self._metadata

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> MemoryKeyValueStoreClient:
        name = name or configuration.default_key_value_store_id

        # Check if the client is already cached by name
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        # If specific id is provided, use it; otherwise, generate a new one
        id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        client = cls(
            id=id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
        )

        # Cache the client by name
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        # Clear all data
        self._records.clear()

        # Remove from cache
        if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
            del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

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
