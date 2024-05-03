from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.models import KeyValueStoreListPage, KeyValueStoreMetadata


class BaseKeyValueStoreCollectionClient(ABC):
    """Abstract base class for key-value store collection clients.

    This collection client handles operations that involve multiple instances of a given resource type.
    """

    @abstractmethod
    async def get_or_create(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        schema: dict | None = None,
    ) -> KeyValueStoreMetadata:
        """Retrieve an existing key-value store by its name or ID, or create a new one if it does not exist.

        Args:
            id: Optional ID of the key-value store to retrieve or create. If provided, the method will attempt
                to find a key-value store with the ID.

            name: Optional name of the key-value store resource to retrieve or create. If provided, the method will
                attempt to find a key-value store with this name.

            schema: Optional schema for the key-value store resource to be created.

        Returns:
            Metadata object containing the information of the retrieved or created key-value store.
        """

    @abstractmethod
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> KeyValueStoreListPage:
        """List the available key-value stores.

        Args:
            unnamed: Whether to list only the unnamed key-value stores.
            limit: Maximum number of key-value stores to return.
            offset: Number of key-value stores to skip from the beginning of the list.
            desc: Whether to sort the key-value stores in descending order.

        Returns:
            The list of available key-value stores matching the specified filters.
        """
