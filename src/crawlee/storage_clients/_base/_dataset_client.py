from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any

    from crawlee.configuration import Configuration
    from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata


@docs_group('Abstract classes')
class DatasetClient(ABC):
    """An abstract class for dataset storage clients.

    Dataset clients provide an interface for accessing and manipulating dataset storage. They handle
    operations like adding and getting dataset items across different storage backends.

    Storage clients are specific to the type of storage they manage (`Dataset`, `KeyValueStore`,
    `RequestQueue`), and can operate with various storage systems including memory, file system,
    databases, and cloud storage solutions.

    This abstract class defines the interface that all specific dataset clients must implement.
    """

    @property
    @abstractmethod
    def metadata(self) -> DatasetMetadata:
        """The metadata of the dataset."""

    @classmethod
    @abstractmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> DatasetClient:
        """Open existing or create a new dataset client.

        If a dataset with the given name or ID already exists, the appropriate dataset client is returned.
        Otherwise, a new dataset is created and client for it is returned.

        The backend method for the `Dataset.open` call.

        Args:
            id: The ID of the dataset. If not provided, an ID may be generated.
            name: The name of the dataset. If not provided a default name may be used.
            configuration: The configuration object.

        Returns:
            A dataset client instance.
        """

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
    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
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
        unwind: str | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate over the dataset items with filtering options.

        The backend method for the `Dataset.iterate_items` call.
        """
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0
