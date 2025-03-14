from __future__ import annotations

import asyncio
import json
import os
import shutil
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pydantic_core._pydantic_core import ValidationError
from typing_extensions import override

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient as BaseDatasetClient
from crawlee.storage_clients._memory._utils import json_dumps
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee._types import JsonSerializable
    from crawlee.configuration import Configuration

logger = getLogger(__name__)


class DatasetClient(BaseDatasetClient):
    """A memory storage implementation of the dataset client."""

    _DEFAULT_NAME = 'default'
    """The name of the unnamed dataset."""

    _STORAGE_SUBDIR = 'datasets'
    """The name of the subdirectory where datasets are stored."""

    _LIST_ITEMS_LIMIT = 999_999_999_999
    """This is what API returns in the x-apify-pagination-limit header when no limit query parameter is used."""

    _LOCAL_ENTRY_NAME_DIGITS = 9
    """Number of characters of the dataset item file names, e.g.: 000000019.json - 9 digits."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        item_count: int,
        storage_dir: str,
    ) -> None:
        """A default constructor.

        Preferably use the `DatasetClient.open` constructor to create a new instance.

        Args:
            client: An instance of a dataset client.
        """
        self._id = id
        self._name = name
        self._created_at = created_at
        self._accessed_at = accessed_at
        self._modified_at = modified_at
        self._item_count = item_count
        self._storage_dir = storage_dir

        # Internal attributes.
        self._lock = asyncio.Lock()
        """A lock to ensure that only one file operation is performed at a time."""

    @override
    @property
    def id(self) -> str:
        return self._id

    @override
    @property
    def name(self) -> str | None:
        return self._name

    @override
    @property
    def created_at(self) -> datetime:
        return self._created_at

    @override
    @property
    def accessed_at(self) -> datetime:
        return self._accessed_at

    @override
    @property
    def modified_at(self) -> datetime:
        return self._modified_at

    @override
    @property
    def item_count(self) -> int:
        return self._item_count

    @property
    def _path_to_dataset(self) -> str:
        return os.path.join(self._storage_dir, self._STORAGE_SUBDIR, self._name)

    @override
    @classmethod
    async def open(cls, id: str | None, name: str | None, configuration: Configuration) -> DatasetClient:
        storage_dir = configuration.storage_dir

        name = name or cls._DEFAULT_NAME
        dataset_path = os.path.join(storage_dir, cls._STORAGE_SUBDIR, name)
        metadata_path = os.path.join(dataset_path, METADATA_FILENAME)

        # If the dataset directory exists, reconstruct the dataset client from the metadata file.
        if os.path.exists(dataset_path):
            # If the metadata file does not exist, raise an error.
            if not os.path.exists(metadata_path):
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

        # Create a new dataset with the given name otherwise.
        else:
            client = cls(
                id=crypto_random_object_id(),
                name=name,
                created_at=datetime.now(timezone.utc),
                accessed_at=datetime.now(timezone.utc),
                modified_at=datetime.now(timezone.utc),
                item_count=0,
                storage_dir=storage_dir,
            )
            await client._update_metadata()

        return client

    @override
    async def drop(self) -> None:
        if os.path.exists(self._path_to_dataset):
            await asyncio.to_thread(shutil.rmtree, self._path_to_dataset)

    @override
    async def push_data(self, data: JsonSerializable) -> None:
        if isinstance(data, list):
            for item in data:
                await self._push_item(item)
        else:
            await self._push_item(data)

    async def _push_item(self, item: dict[str, Any]) -> None:
        self._item_count += 1
        filename = f'{str(self._item_count).zfill(self._LOCAL_ENTRY_NAME_DIGITS)}.json'
        file_path = os.path.join(self._path_to_dataset, filename)

        async with self._lock:
            # Ensure the dataset directory exists.
            await asyncio.to_thread(os.makedirs, self._path_to_dataset, exist_ok=True)

            # Write each normalized item to its own file on disk.
            file = await asyncio.to_thread(open, file_path, mode='w', encoding='utf-8')
            try:
                data_serialized = await json_dumps(item)
                await asyncio.to_thread(file.write, data_serialized)
            finally:
                await asyncio.to_thread(file.close)

        # Update timestamps and persist the metadata.
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def get_data(
        self,
        *,
        offset: int | None = 0,
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
        # Check if any unsupported argument is set (non-default value).
        unsupported_args = [clean, fields, omit, unwind, skip_hidden, flatten, view]
        invalid = [arg for arg in unsupported_args if arg not in (False, None)]
        if invalid:
            logger.warning(
                f'The arguments {invalid} of iterate_items are not supported by the {self.__class__.__name__} client.'
            )

        # Check if the dataset directory exists.
        if not os.path.exists(self._path_to_dataset):
            logger.warning(f'Dataset directory not found: {self._path_to_dataset}')
            return DatasetItemsListPage(
                count=0,
                offset=offset or 0,
                limit=limit or 0,
                total=0,
                desc=desc,
                items=[],
            )

        # Get all JSON files in the dataset directory.
        all_files = await asyncio.to_thread(os.listdir, self._path_to_dataset)
        data_files = [f for f in all_files if f.endswith('.json') and f != METADATA_FILENAME]

        # Sort files numerically (based on the filename without extension).
        data_files.sort(key=lambda f: int(os.path.splitext(f)[0]))
        if desc:
            data_files.reverse()

        # Calculate total number of items and apply offset and limit. Apply offset and limit slicing.
        total = len(data_files)
        start = offset or 0
        selected_files = data_files[start:] if limit is None else data_files[start : start + limit]

        # Read each file one by one.
        items = []
        for filename in selected_files:
            file_path = os.path.join(self._path_to_dataset, filename)

            def read_json_file(fp: str) -> dict:
                with open(fp, encoding='utf-8') as f:
                    return dict(json.load(f))

            try:
                item = await asyncio.to_thread(read_json_file, file_path)
            except Exception:
                logger.exception(f'Error reading {file_path}, skipping the item.')
                continue

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            items.append(item)

        return DatasetItemsListPage(
            count=len(items),
            offset=start,
            limit=limit if limit is not None else total - start,
            total=total,
            desc=desc,
            items=items,
        )

    @override
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
        # Check if any unsupported argument is set (non-default value).
        unsupported_args = [clean, fields, omit, unwind, skip_hidden]
        invalid = [arg for arg in unsupported_args if arg not in (False, None)]
        if invalid:
            logger.warning(
                f'The arguments {invalid} of iterate_items are not supported by the {self.__class__.__name__} client.'
            )

        # Check if the dataset directory exists.
        if not os.path.exists(self._path_to_dataset):
            logger.warning(f'Dataset directory not found: {self._path_to_dataset}')
            return

        # Get all JSON files in the dataset directory.
        all_files = await asyncio.to_thread(os.listdir, self._path_to_dataset)
        data_files = [f for f in all_files if f.endswith('.json') and f != METADATA_FILENAME]

        # Sort files numerically (based on the filename without extension).
        data_files.sort(key=lambda f: int(os.path.splitext(f)[0]))
        if desc:
            data_files.reverse()

        # Apply offset and limit.
        data_files = data_files[offset:]
        if limit is not None:
            data_files = data_files[:limit]

        # Yield each item by reading its JSON file off the event loop.
        for filename in data_files:
            file_path = os.path.join(self._path_to_dataset, filename)

            def read_json_file(file_path: str) -> dict:
                with open(file_path, encoding='utf-8') as f:
                    return dict(json.load(f))

            item = await asyncio.to_thread(read_json_file, file_path)

            # Skip empty items if requested.
            if skip_empty and not item:
                continue

            yield item

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """A helper method to update and persist the dataset metadata."""
        metadata = DatasetMetadata(
            id=self._id,
            name=self._name,
            created_at=self._created_at,
            accessed_at=datetime.now(timezone.utc) if update_accessed_at else self._accessed_at,
            modified_at=datetime.now(timezone.utc) if update_modified_at else self._modified_at,
            item_count=self._item_count,
        )

        metadata_path = os.path.join(self._path_to_dataset, METADATA_FILENAME)
        directory = os.path.dirname(metadata_path)
        await asyncio.to_thread(os.makedirs, directory, exist_ok=True)

        file = await asyncio.to_thread(open, metadata_path, mode='wb+')
        data = metadata.model_dump()

        try:
            string = await json_dumps(data)
            await asyncio.to_thread(file.write, string.encode('utf-8'))
        finally:
            await asyncio.to_thread(file.close)
