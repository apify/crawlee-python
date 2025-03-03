from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.configuration import Configuration
    from crawlee.storage_clients._base import StorageClient
    from crawlee.storage_clients.models import StorageMetadata


class Storage(ABC):
    """Base class for storages."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Get the storage ID."""

    @property
    @abstractmethod
    def name(self) -> str | None:
        """Get the storage name."""

    @property
    @abstractmethod
    def storage_object(self) -> StorageMetadata:
        """Get the full storage object."""

    @storage_object.setter
    @abstractmethod
    def storage_object(self, storage_object: StorageMetadata) -> None:
        """Set the full storage object."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
        storage_client: StorageClient | None = None,
    ) -> Storage:
        """Open a storage, either restore existing or create a new one.

        Args:
            id: The storage ID.
            name: The storage name.
            configuration: Configuration object used during the storage creation or restoration process.
            storage_client: Underlying storage client to use. If not provided, the default global storage client
                from the service locator will be used.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the storage, removing it from the underlying storage client and clearing the cache."""
