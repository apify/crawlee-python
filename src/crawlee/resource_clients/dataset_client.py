from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, AsyncIterator

import aiofiles
import aioshutil
from aiofiles.os import makedirs
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import raise_on_duplicate_storage, raise_on_non_existing_storage
from crawlee._utils.file import force_rename, json_dumps, persist_metadata_if_enabled
from crawlee.resource_clients.base_resource_client import BaseResourceClient
from crawlee.storages.models import DatasetItemsListPage, DatasetMetadata
from crawlee.types import StorageTypes

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.types import JSONSerializable

logger = getLogger(__name__)


class DatasetClient(BaseResourceClient):
    """Sub-client for manipulating a single dataset."""

    _LIST_ITEMS_LIMIT = 999_999_999_999
    """This is what API returns in the x-apify-pagination-limit header when no limit query parameter is used."""

    _LOCAL_ENTRY_NAME_DIGITS = 9
    """Number of characters of the dataset item file names, e.g.: 000000019.json - 9 digits."""

    def __init__(
        self,
        *,
        base_storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
        created_at: datetime | None = None,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
        item_count: int = 0,
    ) -> None:
        self._base_storage_directory = base_storage_directory
        self._memory_storage_client = memory_storage_client
        self.id = id or crypto_random_object_id()
        self.name = name
        self._created_at = created_at or datetime.now(timezone.utc)
        self._accessed_at = accessed_at or datetime.now(timezone.utc)
        self._modified_at = modified_at or datetime.now(timezone.utc)

        self.resource_directory = os.path.join(self._base_storage_directory, self.name or self.id)
        self.dataset_entries: dict[str, dict] = {}
        self.file_operation_lock = asyncio.Lock()
        self.item_count = item_count

    @property
    @override
    def resource_info(self) -> DatasetMetadata:
        """Get the resource info for the dataset client."""
        return DatasetMetadata(
            id=str(self.id),
            name=str(self.name),
            accessed_at=self._accessed_at,
            created_at=self._created_at,
            modified_at=self._modified_at,
            item_count=self.item_count,
        )

    @classmethod
    @override
    def _get_storages_dir(cls, memory_storage_client: MemoryStorageClient) -> str:
        return memory_storage_client.datasets_directory

    @classmethod
    @override
    def _get_storage_client_cache(
        cls,
        memory_storage_client: MemoryStorageClient,
    ) -> list[DatasetClient]:
        return memory_storage_client.datasets_handled

    @classmethod
    @override
    def _create_from_directory(
        cls,
        storage_directory: str,
        memory_storage_client: MemoryStorageClient,
        id: str | None = None,
        name: str | None = None,
    ) -> DatasetClient:
        item_count = 0
        created_at = datetime.now(timezone.utc)
        accessed_at = datetime.now(timezone.utc)
        modified_at = datetime.now(timezone.utc)

        # Load metadata if it exists
        metadata_filepath = os.path.join(storage_directory, '__metadata__.json')

        if os.path.exists(metadata_filepath):
            with open(metadata_filepath, encoding='utf-8') as f:
                json_content = json.load(f)
                resource_info = DatasetMetadata(**json_content)

            id = resource_info.id
            name = resource_info.name
            item_count = resource_info.item_count
            created_at = resource_info.created_at
            accessed_at = resource_info.accessed_at
            modified_at = resource_info.modified_at

        # Load dataset entries
        entries: dict[str, dict] = {}
        has_seen_metadata_file = False

        for entry in os.scandir(storage_directory):
            if entry.is_file():
                if entry.name == '__metadata__.json':
                    has_seen_metadata_file = True
                    continue

                with open(os.path.join(storage_directory, entry.name), encoding='utf-8') as f:
                    entry_content = json.load(f)

                entry_name = entry.name.split('.')[0]
                entries[entry_name] = entry_content

                if not has_seen_metadata_file:
                    item_count += 1

        # Create new dataset client
        new_client = DatasetClient(
            base_storage_directory=memory_storage_client.datasets_directory,
            memory_storage_client=memory_storage_client,
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            item_count=item_count,
        )

        new_client.dataset_entries.update(entries)
        return new_client

    @override
    async def get(self) -> DatasetMetadata | None:
        found = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if found:
            async with found.file_operation_lock:
                await found.update_timestamps(has_been_modified=False)
                return found.resource_info

        return None

    async def update(self, *, name: str | None = None) -> DatasetMetadata:
        """Update the dataset with specified fields.

        Args:
            name: The new name for the dataset

        Returns:
            The updated dataset
        """
        # Check by id
        existing_dataset_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_dataset_by_id is None:
            raise_on_non_existing_storage(StorageTypes.DATASET, self.id)

        # Skip if no changes
        if name is None:
            return existing_dataset_by_id.resource_info

        async with existing_dataset_by_id.file_operation_lock:
            # Check that name is not in use already
            existing_dataset_by_name = next(
                (
                    dataset
                    for dataset in self._memory_storage_client.datasets_handled
                    if dataset.name and dataset.name.lower() == name.lower()
                ),
                None,
            )

            if existing_dataset_by_name is not None:
                raise_on_duplicate_storage(StorageTypes.DATASET, 'name', name)

            existing_dataset_by_id.name = name

            previous_dir = existing_dataset_by_id.resource_directory

            existing_dataset_by_id.resource_directory = os.path.join(
                self._memory_storage_client.datasets_directory, name
            )

            await force_rename(previous_dir, existing_dataset_by_id.resource_directory)

            # Update timestamps
            await existing_dataset_by_id.update_timestamps(has_been_modified=True)

        return existing_dataset_by_id.resource_info

    async def delete(self) -> None:
        """Delete the dataset."""
        dataset = next(
            (dataset for dataset in self._memory_storage_client.datasets_handled if dataset.id == self.id), None
        )

        if dataset is not None:
            async with dataset.file_operation_lock:
                self._memory_storage_client.datasets_handled.remove(dataset)
                dataset.item_count = 0
                dataset.dataset_entries.clear()

                if os.path.exists(dataset.resource_directory):
                    await aioshutil.rmtree(dataset.resource_directory)

    async def list_items(
        self,
        *,
        offset: int | None = 0,
        limit: int | None = _LIST_ITEMS_LIMIT,
        clean: bool = False,  # noqa: ARG002
        desc: bool = False,
        fields: list[str] | None = None,  # noqa: ARG002
        omit: list[str] | None = None,  # noqa: ARG002
        unwind: str | None = None,  # noqa: ARG002
        skip_empty: bool = False,  # noqa: ARG002
        skip_hidden: bool = False,  # noqa: ARG002
        flatten: list[str] | None = None,  # noqa: ARG002
        view: str | None = None,  # noqa: ARG002
    ) -> DatasetItemsListPage:
        """Retrieves a paginated list of items from a dataset based on various filtering parameters.

        This method provides the flexibility to filter, sort, and modify the appearance of dataset items when listed.
        Each parameter modifies the result set according to its purpose. The method also supports pagination
        through 'offset' and 'limit' parameters.

        Args:
            offset: The number of initial items to skip.

            limit: The maximum number of items to return.

            clean: If True, filters out empty items and hidden fields. This is equivalent to setting 'skip_hidden'
                and 'skip_empty' to True.

            desc: If True, items are returned in descending order, i.e., newest first.

            fields: Specifies a subset of fields to include in each item.

            omit: Specifies a subset of fields to exclude from each item.

            unwind: Specifies a field that should be unwound. If it's an array, each element becomes a separate record.

            skip_empty: If True, omits items that are empty after other filters have been applied.

            skip_hidden: If True, omits fields starting with the '#' character.

            flatten: A list of fields to flatten in each item.

            view: The specific view of the dataset to use when retrieving items.

        Returns:
            An object containing the list of filtered, sorted, and paginated dataset items,
                as well as additional pagination details.
        """
        # Check by id
        existing_dataset_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_dataset_by_id is None:
            raise_on_non_existing_storage(StorageTypes.DATASET, self.id)

        async with existing_dataset_by_id.file_operation_lock:
            start, end = existing_dataset_by_id.get_start_and_end_indexes(
                max(existing_dataset_by_id.item_count - (offset or 0) - (limit or self._LIST_ITEMS_LIMIT), 0)
                if desc
                else offset or 0,
                limit,
            )

            items = []

            for idx in range(start, end):
                entry_number = self._generate_local_entry_name(idx)
                items.append(existing_dataset_by_id.dataset_entries[entry_number])

            await existing_dataset_by_id.update_timestamps(has_been_modified=False)

            if desc:
                items.reverse()

            return DatasetItemsListPage(
                count=len(items),
                desc=desc or False,
                items=items,
                limit=limit or self._LIST_ITEMS_LIMIT,
                offset=offset or 0,
                total=existing_dataset_by_id.item_count,
            )

    async def iterate_items(
        self,
        *,
        offset: int = 0,
        limit: int | None = None,
        clean: bool = False,  # noqa: ARG002
        desc: bool = False,
        fields: list[str] | None = None,  # noqa: ARG002
        omit: list[str] | None = None,  # noqa: ARG002
        unwind: str | None = None,  # noqa: ARG002
        skip_empty: bool = False,  # noqa: ARG002
        skip_hidden: bool = False,  # noqa: ARG002
    ) -> AsyncIterator[dict]:
        """Iterates over items in the dataset according to specified filters and sorting.

        This method allows for asynchronously iterating through dataset items while applying various filters such as
        skipping empty items, hiding specific fields, and sorting. It supports pagination via `offset` and `limit`
        parameters, and can modify the appearance of dataset items using `fields`, `omit`, `unwind`, `skip_empty`, and
        `skip_hidden` parameters.

        Args:
            offset: The number of initial items to skip.

            limit: The maximum number of items to iterate over. Defaults to no limit.

            clean: If set to True, filters out empty items and hidden fields. Acts as a shortcut for setting
                `skip_hidden` and `skip_empty` to True.

            desc: If set to True, items are returned in descending order, i.e., newest first.

            fields: Specifies a subset of fields to include in each item.

            omit: Specifies a subset of fields to exclude from each item.

            unwind: Specifies a field that should be unwound into separate items.

            skip_empty: If set to True, omits items that are empty after other filters have been applied.

            skip_hidden: If set to True, omits fields starting with the '#' character from the output.

        Yields:
            An asynchronous iterator of dictionary objects, each representing a dataset item after applying
            the specified filters and transformations.
        """
        cache_size = 1000
        first_item = offset

        # If there is no limit, set last_item to None until we get the total from the first API response
        last_item = None if limit is None else offset + limit

        current_offset = first_item
        while last_item is None or current_offset < last_item:
            current_limit = cache_size if last_item is None else min(cache_size, last_item - current_offset)

            current_items_page = await self.list_items(
                offset=current_offset,
                limit=current_limit,
                desc=desc,
            )

            current_offset += current_items_page.count
            if last_item is None or current_items_page.total < last_item:
                last_item = current_items_page.total

            for item in current_items_page.items:
                yield item

    async def get_items_as_bytes(self, *_args: Any, **_kwargs: Any) -> bytes:
        """Retrieve the items as bytes."""
        raise NotImplementedError('This method is not supported in local memory storage.')

    async def stream_items(self, *_args: Any, **_kwargs: Any) -> AsyncIterator:
        """Stream items from the dataset."""
        raise NotImplementedError('This method is not supported in local memory storage.')

    async def push_items(self, items: JSONSerializable) -> None:
        """Push items to the dataset.

        Args:
            items: The items which to push in the dataset. Either a stringified JSON, a dictionary,
                or a list of strings or dictionaries.
        """
        # Check by id
        existing_dataset_by_id = self.find_or_create_client_by_id_or_name(
            memory_storage_client=self._memory_storage_client,
            id=self.id,
            name=self.name,
        )

        if existing_dataset_by_id is None:
            raise_on_non_existing_storage(StorageTypes.DATASET, self.id)

        normalized = self._normalize_items(items)

        added_ids: list[str] = []
        for entry in normalized:
            existing_dataset_by_id.item_count += 1
            idx = self._generate_local_entry_name(existing_dataset_by_id.item_count)

            existing_dataset_by_id.dataset_entries[idx] = entry
            added_ids.append(idx)

        data_entries = [(id, existing_dataset_by_id.dataset_entries[id]) for id in added_ids]

        async with existing_dataset_by_id.file_operation_lock:
            await existing_dataset_by_id.update_timestamps(has_been_modified=True)

            await self._persist_dataset_items_to_disk(
                data=data_entries,
                entity_directory=existing_dataset_by_id.resource_directory,
                persist_storage=self._memory_storage_client.persist_storage,
            )

    async def _persist_dataset_items_to_disk(
        self,
        *,
        data: list[tuple[str, dict]],
        entity_directory: str,
        persist_storage: bool,
    ) -> None:
        """Writes dataset items to the disk.

        The function iterates over a list of dataset items, each represented as a tuple of an identifier
        and a dictionary, and writes them as individual JSON files in a specified directory. The function
        will skip writing if `persist_storage` is False. Before writing, it ensures that the target
        directory exists, creating it if necessary.

        Args:
            data: A list of tuples, each containing an identifier (string) and a data dictionary.
            entity_directory: The directory path where the dataset items should be stored.
            persist_storage: A boolean flag indicating whether the data should be persisted to the disk.
        """
        # Skip writing files to the disk if the client has the option set to false
        if not persist_storage:
            return

        # Ensure the directory for the entity exists
        await makedirs(entity_directory, exist_ok=True)

        # Save all the new items to the disk
        for idx, item in data:
            file_path = os.path.join(entity_directory, f'{idx}.json')
            async with aiofiles.open(file_path, mode='wb') as f:
                s = await json_dumps(item)
                await f.write(s.encode('utf-8'))

    async def update_timestamps(self, *, has_been_modified: bool) -> None:
        """Update the timestamps of the dataset."""
        self._accessed_at = datetime.now(timezone.utc)

        if has_been_modified:
            self._modified_at = datetime.now(timezone.utc)

        await persist_metadata_if_enabled(
            data=self.resource_info.model_dump(),
            entity_directory=self.resource_directory,
            write_metadata=self._memory_storage_client.write_metadata,
        )

    def get_start_and_end_indexes(self, offset: int, limit: int | None = None) -> tuple[int, int]:
        """Calculate the start and end indexes for listing items."""
        actual_limit = limit or self.item_count
        start = offset + 1
        end = min(offset + actual_limit, self.item_count) + 1
        return (start, end)

    def _generate_local_entry_name(self, idx: int) -> str:
        return str(idx).zfill(self._LOCAL_ENTRY_NAME_DIGITS)

    def _normalize_items(self, items: JSONSerializable) -> list[dict]:
        def normalize_item(item: Any) -> dict | None:
            if isinstance(item, str):
                item = json.loads(item)

            if isinstance(item, list):
                received = ',\n'.join(item)
                raise TypeError(
                    f'Each dataset item can only be a single JSON object, not an array. Received: [{received}]'
                )

            if (not isinstance(item, dict)) and item is not None:
                raise TypeError(f'Each dataset item must be a JSON object. Received: {item}')

            return item

        if isinstance(items, str):
            items = json.loads(items)

        result = list(map(normalize_item, items)) if isinstance(items, list) else [normalize_item(items)]
        # filter(None, ..) returns items that are True
        return list(filter(None, result))
