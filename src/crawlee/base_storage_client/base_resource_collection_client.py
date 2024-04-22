from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.storages.models import BaseListPage, BaseStorageMetadata


class BaseResourceCollectionClient(ABC):
    """Abstract base class for resource collection clients.

    Class for managing collections of storage resources such as datasets, key-value stores, and
    request queues. This client handles operations that involve multiple instances of a given
    resource type.
    """

    @abstractmethod
    async def get_or_create(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        schema: dict | None = None,
    ) -> BaseStorageMetadata:
        """Retrieve an existing storage by its name or ID, or create a new one if it does not exist.

        Args:
            id: Optional ID of the storage to retrieve or create. If provided, the method will attempt
                to find a storage with the ID.

            name: Optional name of the storage resource to retrieve or create. If provided, the method will
                attempt to find a storage with this name.

            schema: Optional schema for the storage resource to be created.

        Returns:
            Metadata object containing the information of the retrieved or created storage.
        """

    @abstractmethod
    async def list(
        self,
        *,
        unnamed: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool | None = None,
    ) -> BaseListPage:
        """List the available storages.

        Args:
            unnamed: Whether to list only the unnamed storages.
            limit: Maximum number of storages to return.
            offset: Number of storages to skip from the beginning of the list.
            desc: Whether to sort the storages in descending order.

        Returns:
            The list of available storages matching the specified filters.
        """
