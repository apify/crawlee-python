from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from crawlee_storage import FileSystemDatasetClient as NativeDatasetClient
from typing_extensions import Self, override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class FileSystemDatasetClient(DatasetClient):
    """File system implementation of the dataset client.

    This client persists dataset items to the file system as individual JSON files within a structured
    directory hierarchy following the pattern:

    ```
    {STORAGE_DIR}/datasets/{DATASET_ID}/{ITEM_ID}.json
    ```

    Each item is stored as a separate file, which allows for durability and the ability to
    recover after process termination. Dataset operations like filtering, sorting, and pagination are
    implemented by processing the stored files according to the requested parameters.

    This implementation is ideal for long-running crawlers where data persistence is important,
    and for development environments where you want to easily inspect the collected data between runs.

    Backed by the native ``crawlee_storage`` Rust extension for performance.
    """

    def __init__(
        self,
        *,
        native_client: NativeDatasetClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemDatasetClient.open` class method to create a new instance.
        """
        self._native_client = native_client

    @property
    def path_to_dataset(self) -> Path:
        """The full path to the dataset directory."""
        return self._native_client.path_to_dataset

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the dataset metadata file."""
        return self._native_client.path_to_metadata

    @override
    async def get_metadata(self) -> DatasetMetadata:
        raw = await self._native_client.get_metadata()
        return DatasetMetadata(**raw)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        configuration: Configuration,
    ) -> Self:
        """Open or create a file system dataset client.

        This method attempts to open an existing dataset from the file system. If a dataset with the specified ID
        or name exists, it loads the metadata from the stored files. If no existing dataset is found, a new one
        is created.

        Args:
            id: The ID of the dataset to open. If provided, searches for existing dataset by ID.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a dataset with the specified ID is not found, if metadata is invalid,
                or if both name and alias are provided.
        """
        native_client = await NativeDatasetClient.open(
            id=id,
            name=name,
            alias=alias,
            storage_dir=str(configuration.storage_dir),
        )

        return cls(native_client=native_client)

    @override
    async def drop(self) -> None:
        await self._native_client.drop_storage()

    @override
    async def purge(self) -> None:
        await self._native_client.purge()

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        await self._native_client.push_data(data)

    @override
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
        # Check for unsupported arguments and log a warning if found.
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
            'flatten': flatten,
            'view': view,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}

        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of get_data are not supported by the '
                f'{self.__class__.__name__} client.'
            )

        raw = await self._native_client.get_data(
            offset=offset,
            limit=limit if limit is not None else 999_999_999_999,
            desc=desc,
            skip_empty=skip_empty,
        )
        return DatasetItemsListPage(**raw)

    @override
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
    ) -> AsyncIterator[dict[str, Any]]:
        # Check for unsupported arguments and log a warning if found.
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}

        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of iterate are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        async for item in self._native_client.iterate_items(
            offset=offset,
            limit=limit,
            desc=desc,
            skip_empty=skip_empty,
        ):
            yield item
