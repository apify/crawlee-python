from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata


@docs_group('Abstract classes')
class KeyValueStoreClient(ABC):
    """An abstract class for key-value store (KVS) storage clients.

    Key-value stores clients provide an interface for accessing and manipulating KVS storage. They handle
    operations like getting, setting, deleting KVS values across different storage backends.

    Storage clients are specific to the type of storage they manage (`Dataset`, `KeyValueStore`,
    `RequestQueue`), and can operate with various storage systems including memory, file system,
    databases, and cloud storage solutions.

    This abstract class defines the interface that all specific KVS clients must implement.
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
        """Retrieve the given record from the key-value store.

        The backend method for the `KeyValueStore.get_value` call.
        """

    @abstractmethod
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value in the key-value store by its key.

        The backend method for the `KeyValueStore.set_value` call.
        """

    @abstractmethod
    async def delete_value(self, *, key: str) -> None:
        """Delete a value from the key-value store by its key.

        The backend method for the `KeyValueStore.delete_value` call.
        """

    @abstractmethod
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        """Iterate over all the existing keys in the key-value store.

        The backend method for the `KeyValueStore.iterate_keys` call.
        """
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0

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
