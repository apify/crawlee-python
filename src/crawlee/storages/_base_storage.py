from __future__ import annotations

from abc import ABC, abstractmethod


class BaseStorage(ABC):
    """Base class for storages."""

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
    async def open(cls, *, id: str | None = None, name: str | None = None) -> BaseStorage:
        """Open a storage, either restore existing or create a new one.

        Args:
            id: The storage ID.
            name: The storage name.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the storage. Remove it from underlying storage and delete from cache."""
