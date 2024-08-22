from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncContextManager

if TYPE_CHECKING:
    from httpx import Response

    from crawlee.models import KeyValueStoreListKeysPage, KeyValueStoreMetadata, KeyValueStoreRecord


class BaseKeyValueStoreClient(ABC):
    """Abstract base class for key-value store resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @abstractmethod
    async def get(self) -> KeyValueStoreMetadata | None:
        """Get metadata about the key-value store being managed by this client.

        Returns:
            An object containing the key-value store's details, or None if the key-value store does not exist.
        """

    @abstractmethod
    async def update(
        self,
        *,
        name: str | None = None,
    ) -> KeyValueStoreMetadata:
        """Update the key-value store metadata.

        Args:
            name: New new name for the key-value store.

        Returns:
            An object reflecting the updated key-value store metadata.
        """

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the key-value store managed by this client."""

    @abstractmethod
    async def list_keys(
        self,
        *,
        limit: int = 1000,
        exclusive_start_key: str | None = None,
    ) -> KeyValueStoreListKeysPage:
        """List the keys in the key-value store.

        Args:
            limit: Number of keys to be returned. Maximum value is 1000.
            exclusive_start_key: All keys up to this one (including) are skipped from the result.

        Returns:
            The list of keys in the key-value store matching the given arguments.
        """

    @abstractmethod
    async def get_record(self, key: str) -> KeyValueStoreRecord | None:
        """Retrieve the given record from the key-value store.

        Args:
            key: Key of the record to retrieve

        Returns:
            The requested record, or None, if the record does not exist
        """

    @abstractmethod
    async def get_record_as_bytes(self, key: str) -> KeyValueStoreRecord[bytes] | None:
        """Retrieve the given record from the key-value store, without parsing it.

        Args:
            key: Key of the record to retrieve

        Returns:
            The requested record, or None, if the record does not exist
        """

    @abstractmethod
    async def stream_record(self, key: str) -> AsyncContextManager[KeyValueStoreRecord[Response] | None]:
        """Retrieve the given record from the key-value store, as a stream.

        Args:
            key: Key of the record to retrieve

        Returns:
            The requested record as a context-managed streaming Response, or None, if the record does not exist
        """

    @abstractmethod
    async def set_record(self, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value to the given record in the key-value store.

        Args:
            key: The key of the record to save the value to
            value: The value to save into the record
            content_type: The content type of the saved value
        """

    @abstractmethod
    async def delete_record(self, key: str) -> None:
        """Delete the specified record from the key-value store.

        Args:
            key: The key of the record which to delete
        """
