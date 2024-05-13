from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.configuration import Configuration


class BaseStorage(ABC):
    """Base class for storages."""

    LABEL = 'Unknown'
    """Human readable label of the storage."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Get the storage ID."""

    @property
    @abstractmethod
    def name(self) -> str | None:
        """Get the storage name."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        configuration: Configuration | None = None,
    ) -> BaseStorage:
        """Open a storage, either restore existing or create a new one.

        Args:
            id: The storage ID.
            name: The storage name.
            configuration: The configuration to use.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the storage. Remove it from underlying storage and delete from cache."""
