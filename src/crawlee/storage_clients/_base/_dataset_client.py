from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime
    from pathlib import Path
    from typing import Any

    from crawlee.storage_clients.models import DatasetItemsListPage


@docs_group('Abstract classes')
class DatasetClient(ABC):
    """An abstract class for dataset resource clients.

    These clients are specific to the type of resource they manage and operate under a designated storage
    client, like a memory storage client.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """The ID of the dataset."""

    @property
    @abstractmethod
    def name(self) -> str | None:
        """The name of the dataset."""

    @property
    @abstractmethod
    def created_at(self) -> datetime:
        """The time at which the dataset was created."""

    @property
    @abstractmethod
    def accessed_at(self) -> datetime:
        """The time at which the dataset was last accessed."""

    @property
    @abstractmethod
    def modified_at(self) -> datetime:
        """The time at which the dataset was last modified."""

    @property
    @abstractmethod
    def item_count(self) -> int:
        """The number of items in the dataset."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_dir: Path,
    ) -> DatasetClient:
        """Open existing or create a new dataset client.

        If a dataset with the given name already exists, the appropriate dataset client is returned.
        Otherwise, a new dataset is created and client for it is returned.

        Args:
            id: The ID of the dataset.
            name: The name of the dataset.
            storage_dir: The path to the storage directory. If the client persists data, it should use this directory.

        Returns:
            A dataset client.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole dataset and remove all its items.

        The backend method for the `Dataset.drop` call.
        """

    @abstractmethod
    async def push_data(self, *, data: list[Any] | dict[str, Any]) -> None:
        """Push data to the dataset.

        The backend method for the `Dataset.push_data` call.
        """

    @abstractmethod
    async def get_data(
        self,
        *,
        offset: int = 0,
        limit: int | None = 999_999_999_999,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: list[str] | None = None,
        view: str | None = None,
    ) -> DatasetItemsListPage:
        """Get data from the dataset.

        The backend method for the `Dataset.get_data` call.
        """

    @abstractmethod
    async def iterate(
        self,
        *,
        offset: int = 0,
        limit: int | None = None,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict]:
        """Iterate over the dataset.

        The backend method for the `Dataset.iterate` call.
        """
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0
