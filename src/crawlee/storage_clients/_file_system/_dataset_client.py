from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from pydantic import ValidationError
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._utils import METADATA_FILENAME, json_dumps

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any

logger = getLogger(__name__)


class FileSystemDatasetClient(DatasetClient):
    """A file system implementation of the dataset client.

    This client persists data to the file system, making it suitable for scenarios where data needs
    to survive process restarts. Each dataset item is stored as a separate JSON file with a numeric
    filename, allowing for easy ordering and pagination.
    """

    _DEFAULT_NAME = 'default'
    """The default name for the dataset when no name is provided."""

    _STORAGE_SUBDIR = 'datasets'
    """The name of the subdirectory where datasets are stored."""

    _LOCAL_ENTRY_NAME_DIGITS = 9
    """Number of digits used for the file names (e.g., 000000019.json)."""

    _cache_by_name: ClassVar[dict[str, FileSystemDatasetClient]] = {}
    """A dictionary to cache clients by their names."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        item_count: int,
        storage_dir: Path,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `FileSystemDatasetClient.open` class method to create a new instance.
        """
        self._metadata = DatasetMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            item_count=item_count,
        )

        self._storage_dir = storage_dir

        # Internal attributes
        self._lock = asyncio.Lock()
        """A lock to ensure that only one file operation is performed at a time."""

    @override
    @property
    def metadata(self) -> DatasetMetadata:
        return self._metadata

    @property
    def path_to_dataset(self) -> Path:
        """The full path to the dataset directory."""
        return self._storage_dir / self._STORAGE_SUBDIR / self.metadata.name

    @property
    def path_to_metadata(self) -> Path:
        """The full path to the dataset metadata file."""
        return self.path_to_dataset / METADATA_FILENAME

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        storage_dir: Path | None = None,
    ) -> FileSystemDatasetClient:
        if id:
            raise ValueError(
                'Opening a dataset by "id" is not supported for file system storage client, use "name" instead.'
            )

        name = name or cls._DEFAULT_NAME

        # Check if the client is already cached by name.
        if name in cls._cache_by_name:
            client = cls._cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        storage_dir = storage_dir or Path.cwd()
        dataset_path = storage_dir / cls._STORAGE_SUBDIR / name
        metadata_path = dataset_path / METADATA_FILENAME

        # If the dataset directory exists, reconstruct the client from the metadata file.
        if dataset_path.exists():
            # If metadata file is missing, raise an error.
            if not metadata_path.exists():
                raise ValueError(f'Metadata file not found for dataset "{name}"')

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
                id=metadata.id,
                name=name,
                created_at=metadata.created_at,
                accessed_at=metadata.accessed_at,
                modified_at=metadata.modified_at,
                item_count=metadata.item_count,
                storage_dir=storage_dir,
            )

            await client._update_metadata(update_accessed_at=True)

        # Otherwise, create a new dataset client.
        else:
            now = datetime.now(timezone.utc)
            client = cls(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                item_count=0,
                storage_dir=storage_dir,
            )
            await client._update_metadata()

        # Cache the client by name.
        cls._cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        # If the client directory exists, remove it recursively.
        if self.path_to_dataset.exists():
            async with self._lock:
                await asyncio.to_thread(shutil.rmtree, self.path_to_dataset)

        # Remove the client from the cache.
        if self.metadata.name in self.__class__._cache_by_name:  # noqa: SLF001
            del self.__class__._cache_by_name[self.metadata.name]  # noqa: SLF001

    @override
    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
        new_item_count = self.metadata.item_count

        # If data is a list, push each item individually.
        if isinstance(data, list):
            for item in data:
                new_item_count += 1
                await self._push_item(item, new_item_count)
        else:
            new_item_count += 1
            await self._push_item(data, new_item_count)

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
        unsupported_args = {
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
        data_files = await self._get_sorted_data_files()
        total = len(data_files)

        # Reverse the order if descending order is requested.
        if desc:
            data_files.reverse()

        # Apply offset and limit slicing.
        selected_files = data_files[offset:]
        if limit is not None:
            selected_files = selected_files[:limit]

        # Read and parse each data file.
        items = []
        for file_path in selected_files:
            try:
                file_content = await asyncio.to_thread(file_path.read_text, encoding='utf-8')
                item = json.loads(file_content)
            except Exception:
                logger.exception(f'Error reading {file_path}, skipping the item.')
                continue

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            items.append(item)

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
    ) -> AsyncIterator[dict]:
        # Check for unsupported arguments and log a warning if found.
        unsupported_args = {
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
        data_files = await self._get_sorted_data_files()

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
                item = json.loads(file_content)
            except Exception:
                logger.exception(f'Error reading {file_path}, skipping the item.')
                continue

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            yield item

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
        await asyncio.to_thread(self.path_to_metadata.write_text, data, encoding='utf-8')

    async def _push_item(self, item: dict[str, Any], item_id: int) -> None:
        """Push a single item to the dataset.

        This method writes the item as a JSON file with a zero-padded numeric filename
        that reflects its position in the dataset sequence.

        Args:
            item: The data item to add to the dataset.
            item_id: The sequential ID to use for this item's filename.
        """
        # Acquire the lock to perform file operations safely.
        async with self._lock:
            # Generate the filename for the new item using zero-padded numbering.
            filename = f'{str(item_id).zfill(self._LOCAL_ENTRY_NAME_DIGITS)}.json'
            file_path = self.path_to_dataset / filename

            # Ensure the dataset directory exists.
            await asyncio.to_thread(self.path_to_dataset.mkdir, parents=True, exist_ok=True)

            # Dump the serialized item to the file.
            data = await json_dumps(item)
            await asyncio.to_thread(file_path.write_text, data, encoding='utf-8')

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
