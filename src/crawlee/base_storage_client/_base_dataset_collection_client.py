from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.models import DatasetListPage, DatasetMetadata


class BaseDatasetCollectionClient(ABC):
    """Abstract base class for dataset collection clients.

    This collection client handles operations that involve multiple instances of a given resource type.
    """

    @abstractmethod
    async def get_or_create(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        schema: dict | None = None,
    ) -> DatasetMetadata:
        """Retrieve an existing dataset by its name or ID, or create a new one if it does not exist.

        Args:
            id: Optional ID of the dataset to retrieve or create. If provided, the method will attempt
                to find a dataset with the ID.

            name: Optional name of the dataset resource to retrieve or create. If provided, the method will
                attempt to find a dataset with this name.

            schema: Optional schema for the dataset resource to be created.

        Returns:
            Metadata object containing the information of the retrieved or created dataset.
        """

    @abstractmethod
    async def list(
        self,
        *,
        unnamed: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        desc: bool = False,
    ) -> DatasetListPage:
        """List the available datasets.

        Args:
            unnamed: Whether to list only the unnamed datasets.
            limit: Maximum number of datasets to return.
            offset: Number of datasets to skip from the beginning of the list.
            desc: Whether to sort the datasets in descending order.

        Returns:
            The list of available datasets matching the specified filters.
        """
