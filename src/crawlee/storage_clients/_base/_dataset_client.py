from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from crawlee._types import JsonSerializable, PushDataKwargs
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from crawlee.configuration import Configuration
    from crawlee.storage_clients.models import DatasetItemsListPage
    from crawlee.storages._types import GetDataKwargs, IterateKwargs


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
    async def open(cls, id: str | None, name: str | None, configuration: Configuration) -> DatasetClient:
        """Open existing or create a new dataset client.

        If a dataset with the given name already exists, the appropriate dataset client is returned.
        Otherwise, a new dataset is created and client for it is returned.

        Args:
            id: The ID of the dataset.
            name: The name of the dataset.
            configuration: The configuration object.

        Returns:
            A dataset client.
        """

    @abstractmethod
    async def drop(self) -> None:
        """Implementation of the `Dataset.drop` method."""

    @abstractmethod
    async def push_data(self, data: JsonSerializable, **kwargs: PushDataKwargs) -> None:
        """Implementation of the `Dataset.push_data` method."""

    @abstractmethod
    async def get_data(self, **kwargs: GetDataKwargs) -> DatasetItemsListPage:
        """Implementation of the `Dataset.get_data` method."""

    @abstractmethod
    async def iterate(self, **kwargs: IterateKwargs) -> AsyncIterator[dict]:
        """Implementation of the `Dataset.iterate` method."""
        # This syntax is to make mypy properly work with abstract AsyncIterator.
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        raise NotImplementedError
        if False:  # type: ignore[unreachable]
            yield 0
