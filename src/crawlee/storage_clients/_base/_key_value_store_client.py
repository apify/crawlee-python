from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime
    from pathlib import Path

    from crawlee.storage_clients.models import (
        KeyValueStoreListKeysPage,
        KeyValueStoreRecord,
        KeyValueStoreRecordMetadata,
    )

# Properties:
# - id
# - name
# - created_at
# - accessed_at
# - modified_at

# Methods:
# - open
# - drop
# - get_value
# - set_value
# - delete_value
# - iterate_keys
# - get_public_url


@docs_group('Abstract classes')
class KeyValueStoreClient(ABC):
    """An abstract class for key-value store (KVS) resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """The ID of the key-value store."""

    @property
    @abstractmethod
    def name(self) -> str | None:
        """The name of the key-value store."""

    @property
    @abstractmethod
    def created_at(self) -> datetime:
        """The time at which the key-value store was created."""

    @property
    @abstractmethod
    def accessed_at(self) -> datetime:
        """The time at which the key-value store was last accessed."""

    @property
    @abstractmethod
    def modified_at(self) -> datetime:
        """The time at which the key-value store was last modified."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_dir: Path,
    ) -> KeyValueStoreClient:
        """Open existing or create a new key-value store client.

        If a key-value store with the given name already exists, the appropriate key-value store client is returned.
        Otherwise, a new key-value store is created and client for it is returned.

        Args:
            id: The ID of the key-value store.
            name: The name of the key-value store.
            storage_dir: The path to the storage directory. If the client persists data, it should use this directory.

        Returns:
            A key-value store client.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole key-value store and remove all its values.

        The backend method for the `KeyValueStore.drop` call.
        """

    @abstractmethod
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        """Retrieve the given record from the key-value store.

        Args:
            key: Key of the record to retrieve.

        Returns:
            The requested record, or None, if the record does not exist
        """

    @abstractmethod
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value to the given record in the key-value store.

        Args:
            key: The key of the record to save the value to.
            value: The value to save into the record.
            content_type: The MIME content type string. If not provided, it is inferred from the value.
        """

    @abstractmethod
    async def delete_value(self, *, key: str) -> None:
        """Delete the specified record from the key-value store.

        Args:
            key: The key of the record which to delete.
        """

    @abstractmethod
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        """Iterate over the existing keys in the key-value store.

        The backend method for the `KeyValueStore.iterate_keys` call.
        """
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0

    @abstractmethod
    async def list_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreListKeysPage:
        """List the keys in the key-value store.

        Args:
            exclusive_start_key: All keys up to this one (including) are skipped from the result.
            limit: Number of keys to be returned.

        Returns:
            The list of keys in the key-value store matching the given arguments.
        """

    @abstractmethod
    async def get_public_url(self, *, key: str) -> str:
        """Get the public URL for the given key.

        Args:
            key: Key of the record for which URL is required.

        Returns:
            The public URL for the given key.
        """
