from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from crawlee._types import JsonSerializable
    from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata


class DatasetClient(ABC):
    """An abstract class for dataset storage clients.

    Dataset clients provide an interface for accessing and manipulating dataset storage. They handle
    operations like adding and getting dataset items across different storage backends.

    Storage clients are specific to the type of storage they manage (`Dataset`, `KeyValueStore`,
    `RequestQueue`), and can operate with various storage systems including memory, file system,
    databases, and cloud storage solutions.

    This abstract class defines the interface that all specific dataset clients must implement.
    """

    @abstractmethod
    async def get_metadata(self) -> DatasetMetadata:
        """Get the metadata of the dataset."""

    @abstractmethod
    async def drop(self) -> None:
        """Drop the whole dataset and remove all its items.

        The backend method for the `Dataset.drop` call.
        """

    @abstractmethod
    async def purge(self) -> None:
        """Purge all items from the dataset.

        The backend method for the `Dataset.purge` call.
        """

    @abstractmethod
    async def push_data(self, data: Sequence[Mapping[str, JsonSerializable]] | Mapping[str, JsonSerializable]) -> None:
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
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: list[str] | None = None,
        view: str | None = None,
    ) -> DatasetItemsListPage:
        """Get data from the dataset with various filtering options.

        The backend method for the `Dataset.get_data` call.
        """

    @abstractmethod
    async def iterate_items(
        self,
        *,
        offset: int = 0,
        limit: int | None = None,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[Mapping[str, JsonSerializable]]:
        """Iterate over the dataset items with filtering options.

        The backend method for the `Dataset.iterate_items` call.
        """
        # This syntax is to make type checker properly work with abstract AsyncIterator.
        raise NotImplementedError
        if False:
            yield {}

    @staticmethod
    def _is_sequence_of_items(
        data: Sequence[Mapping[str, JsonSerializable]] | Mapping[str, JsonSerializable],
    ) -> bool:
        """Tell whether the `push_data` payload is a sequence of items rather than a single item.

        No `push_data` implementation in this repository uses this helper - it is inlined there instead. It stays
        on the base class because the Apify SDK up to 4.0.1 calls it from `ApifyDatasetClient.push_data`, so
        removing it breaks every Actor that resolves such an SDK version against this package. Drop it in the
        next major version, once those SDK versions are out of the supported range.
        """
        return isinstance(data, Sequence)
