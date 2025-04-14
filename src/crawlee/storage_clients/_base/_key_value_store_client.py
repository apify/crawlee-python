from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

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

    @property
    @abstractmethod
    def metadata(self) -> KeyValueStoreMetadata:
        """The metadata of the key-value store."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        storage_dir: Path | None = None,
    ) -> KeyValueStoreClient:
        """Open existing or create a new key-value store client.

        If a key-value store with the given name or ID already exists, the appropriate
        key-value store client is returned. Otherwise, a new key-value store is created
        and a client for it is returned.

        The backend method for the `KeyValueStoreClient.open` call.

        Args:
            id: The ID of the key-value store. If not provided, an ID may be generated.
            name: The name of the key-value store. If not provided a default name may be used.
            storage_dir: The path to the storage directory. If the client persists data,
                it should use this directory. May be ignored by non-persistent implementations.

        Returns:
            A key-value store client instance.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole key-value store and remove all its values.

        The backend method for the `KeyValueStore.drop` call.
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
