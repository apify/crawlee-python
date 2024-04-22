from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.storages.models import BaseStorageMetadata


class BaseResourceClient(ABC):
    """Abstract base class for resource clients.

    Class for managing storage-based resources such as datasets, key-value stores, and request queues.
    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @abstractmethod
    async def get(self) -> BaseStorageMetadata | None:
        """Get metadata about the storage resource being managed by this client.

        Returns:
            An object containing the resource's details, or None if the resource does not exist.
        """

    @abstractmethod
    async def update(
        self,
        *,
        name: str | None = None,
    ) -> BaseStorageMetadata:
        """Update the storage resource metadata.

        Args:
            name: New new name for the storage resource.

        Returns:
            An object reflecting the updated storage resource details.
        """

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the storage resource managed by this client."""
