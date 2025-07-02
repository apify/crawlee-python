from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from typing_extensions import override

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import atomic_write, json_dumps
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

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
    """

    _STORAGE_SUBDIR = 'datasets'
    """The name of the subdirectory where datasets are stored."""

    _STORAGE_SUBSUBDIR_DEFAULT = 'default'
    """The name of the subdirectory for the default dataset."""

    _ITEM_FILENAME_DIGITS = 9
    """Number of digits used for the dataset item file names (e.g., 000000019.json)."""

    def __init__(
        self,
        *,
        metadata: DatasetMetadata,
        storage_dir: Path,
        lock: asyncio.Lock,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemDatasetClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._storage_dir = storage_dir
        """The base directory where the storage data are being persisted."""

        self._lock = lock
        """A lock to ensure that only one operation is performed at a time."""

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return self._metadata

    @property
    def path_to_dataset(self) -> Path:
        """The full path to the dataset directory."""
        if self._metadata.name is None:
            return self._storage_dir / self._STORAGE_SUBDIR / self._STORAGE_SUBSUBDIR_DEFAULT

        return self._storage_dir / self._STORAGE_SUBDIR / self._metadata.name

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the dataset metadata file."""
        return self.path_to_dataset / METADATA_FILENAME

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        configuration: Configuration,
    ) -> FileSystemDatasetClient:
        """Open or create a file system dataset client.

        This method attempts to open an existing dataset from the file system. If a dataset with the specified ID
        or name exists, it loads the metadata from the stored files. If no existing dataset is found, a new one
        is created.

        Args:
            id: The ID of the dataset to open. If provided, searches for existing dataset by ID.
            name: The name of the dataset to open. If not provided, uses the default dataset.
            configuration: The configuration object containing storage directory settings.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a dataset with the specified ID is not found, or if metadata is invalid.
        """
        storage_dir = Path(configuration.storage_dir)
        dataset_base_path = storage_dir / cls._STORAGE_SUBDIR

        if not dataset_base_path.exists():
            await asyncio.to_thread(dataset_base_path.mkdir, parents=True, exist_ok=True)

        # Get a new instance by ID.
        if id:
            found = False
            for dataset_dir in dataset_base_path.iterdir():
                if not dataset_dir.is_dir():
                    continue

                metadata_path = dataset_dir / METADATA_FILENAME
                if not metadata_path.exists():
                    continue

                try:
                    file = await asyncio.to_thread(metadata_path.open)
                    try:
                        file_content = json.load(file)
                        metadata = DatasetMetadata(**file_content)
                        if metadata.id == id:
                            client = cls(
                                metadata=metadata,
                                storage_dir=storage_dir,
                                lock=asyncio.Lock(),
                            )
                            await client._update_metadata(update_accessed_at=True)
                            found = True
                            break
                    finally:
                        await asyncio.to_thread(file.close)
                except (json.JSONDecodeError, ValidationError):
                    continue

            if not found:
                raise ValueError(f'Dataset with ID "{id}" not found')

        # Get a new instance by name.
        else:
            dataset_path = (
                dataset_base_path / cls._STORAGE_SUBSUBDIR_DEFAULT if name is None else dataset_base_path / name
            )
            metadata_path = dataset_path / METADATA_FILENAME

            # If the dataset directory exists, reconstruct the client from the metadata file.
            if dataset_path.exists() and metadata_path.exists():
                file = await asyncio.to_thread(open, metadata_path)
                try:
                    file_content = json.load(file)
                finally:
                    await asyncio.to_thread(file.close)
                try:
                    metadata = DatasetMetadata(**file_content)
                except ValidationError as exc:
                    raise ValueError(f'Invalid metadata file for dataset "{name}"') from exc

                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )

                await client._update_metadata(update_accessed_at=True)

            # Otherwise, create a new dataset client.
            else:
                now = datetime.now(timezone.utc)
                metadata = DatasetMetadata(
                    id=crypto_random_object_id(),
                    name=name,
                    created_at=now,
                    accessed_at=now,
                    modified_at=now,
                    item_count=0,
                )
                client = cls(
                    metadata=metadata,
                    storage_dir=storage_dir,
                    lock=asyncio.Lock(),
                )
                await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        async with self._lock:
            if self.path_to_dataset.exists():
                await asyncio.to_thread(shutil.rmtree, self.path_to_dataset)

    @override
    async def purge(self) -> None:
        async with self._lock:
            for file_path in await self._get_sorted_data_files():
                await asyncio.to_thread(file_path.unlink, missing_ok=True)

            await self._update_metadata(
                update_accessed_at=True,
                update_modified_at=True,
                new_item_count=0,
            )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        async with self._lock:
            new_item_count = self._metadata.item_count
            if isinstance(data, list):
                for item in data:
                    new_item_count += 1
                    await self._push_item(item, new_item_count)
            else:
                new_item_count += 1
                await self._push_item(data, new_item_count)

            # now update metadata under the same lock
            await self._update_metadata(
                update_accessed_at=True,
                update_modified_at=True,
                new_item_count=new_item_count,
            )

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
        unwind: str | None = None,
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

        # If the dataset directory does not exist, log a warning and return an empty page.
        if not self.path_to_dataset.exists():
            logger.warning(f'Dataset directory not found: {self.path_to_dataset}')
            return DatasetItemsListPage(
                count=0,
                offset=offset,
                limit=limit or 0,
                total=0,
                desc=desc,
                items=[],
            )

        # Get the list of sorted data files.
        async with self._lock:
            try:
                data_files = await self._get_sorted_data_files()
            except FileNotFoundError:
                # directory was dropped mid-check
                return DatasetItemsListPage(count=0, offset=offset, limit=limit or 0, total=0, desc=desc, items=[])

        total = len(data_files)

        # Reverse the order if descending order is requested.
        if desc:
            data_files.reverse()

        # Apply offset and limit slicing.
        selected_files = data_files[offset:]
        if limit is not None:
            selected_files = selected_files[:limit]

        # Read and parse each data file.
        items = list[dict[str, Any]]()
        for file_path in selected_files:
            try:
                file_content = await asyncio.to_thread(file_path.read_text, encoding='utf-8')
            except FileNotFoundError:
                logger.warning(f'File disappeared during iterate_items(): {file_path}, skipping')
                continue

            try:
                item = json.loads(file_content)
            except json.JSONDecodeError:
                logger.exception(f'Corrupt JSON in {file_path}, skipping')
                continue

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            items.append(item)

        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

        # Return a paginated list page of dataset items.
        return DatasetItemsListPage(
            count=len(items),
            offset=offset,
            limit=limit or total - offset,
            total=total,
            desc=desc,
            items=items,
        )

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
        unwind: str | None = None,
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

        # If the dataset directory does not exist, log a warning and return immediately.
        if not self.path_to_dataset.exists():
            logger.warning(f'Dataset directory not found: {self.path_to_dataset}')
            return

        # Get the list of sorted data files.
        async with self._lock:
            try:
                data_files = await self._get_sorted_data_files()
            except FileNotFoundError:
                return

        # Reverse the order if descending order is requested.
        if desc:
            data_files.reverse()

        # Apply offset and limit slicing.
        selected_files = data_files[offset:]
        if limit is not None:
            selected_files = selected_files[:limit]

        # Iterate over each data file, reading and yielding its parsed content.
        for file_path in selected_files:
            try:
                file_content = await asyncio.to_thread(file_path.read_text, encoding='utf-8')
            except FileNotFoundError:
                logger.warning(f'File disappeared during iterate_items(): {file_path}, skipping')
                continue

            try:
                item = json.loads(file_content)
            except json.JSONDecodeError:
                logger.exception(f'Corrupt JSON in {file_path}, skipping')
                continue

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            yield item

        async with self._lock:
            await self._update_metadata(update_accessed_at=True)

    async def _update_metadata(
        self,
        *,
        new_item_count: int | None = None,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata file with current information.

        Args:
            new_item_count: If provided, update the item count to this value.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._metadata.accessed_at = now
        if update_modified_at:
            self._metadata.modified_at = now
        if new_item_count is not None:
            self._metadata.item_count = new_item_count

        # Ensure the parent directory for the metadata file exists.
        await asyncio.to_thread(self.path_to_metadata.parent.mkdir, parents=True, exist_ok=True)

        # Dump the serialized metadata to the file.
        data = await json_dumps(self._metadata.model_dump())
        await atomic_write(self.path_to_metadata, data)

    async def _push_item(self, item: dict[str, Any], item_id: int) -> None:
        """Push a single item to the dataset.

        This method writes the item as a JSON file with a zero-padded numeric filename
        that reflects its position in the dataset sequence.

        Args:
            item: The data item to add to the dataset.
            item_id: The sequential ID to use for this item's filename.
        """
        # Generate the filename for the new item using zero-padded numbering.
        filename = f'{str(item_id).zfill(self._ITEM_FILENAME_DIGITS)}.json'
        file_path = self.path_to_dataset / filename

        # Ensure the dataset directory exists.
        await asyncio.to_thread(self.path_to_dataset.mkdir, parents=True, exist_ok=True)

        # Dump the serialized item to the file.
        data = await json_dumps(item)
        await atomic_write(file_path, data)

    async def _get_sorted_data_files(self) -> list[Path]:
        """Retrieve and return a sorted list of data files in the dataset directory.

        The files are sorted numerically based on the filename (without extension),
        which corresponds to the order items were added to the dataset.

        Returns:
            A list of `Path` objects pointing to data files, sorted by numeric filename.
        """
        # Retrieve and sort all JSON files in the dataset directory numerically.
        files = await asyncio.to_thread(
            sorted,
            self.path_to_dataset.glob('*.json'),
            key=lambda f: int(f.stem) if f.stem.isdigit() else 0,
        )

        # Remove the metadata file from the list if present.
        if self.path_to_metadata in files:
            files.remove(self.path_to_metadata)

        return files
