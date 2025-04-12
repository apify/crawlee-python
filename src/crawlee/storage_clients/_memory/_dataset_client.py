from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

logger = getLogger(__name__)

_cache_by_name = dict[str, 'MemoryDatasetClient']()
"""A dictionary to cache clients by their names."""


class MemoryDatasetClient(DatasetClient):
    """A memory implementation of the dataset client.

    This client stores dataset items in memory using a list. No data is persisted, which means
    all data is lost when the process terminates. This implementation is mainly useful for testing
    and development purposes where persistence is not required.
    """

    _DEFAULT_NAME = 'default'
    """The default name for the dataset when no name is provided."""

    def __init__(
        self,
        *,
        id: str,
        name: str,
        created_at: datetime,
        accessed_at: datetime,
        modified_at: datetime,
        item_count: int,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        self._metadata = DatasetMetadata(
            id=id,
            name=name,
            created_at=created_at,
            accessed_at=accessed_at,
            modified_at=modified_at,
            item_count=item_count,
        )

        # List to hold dataset items
        self._records = list[dict[str, Any]]()

    @override
    @property
    def id(self) -> str:
        return self._metadata.id

    @override
    @property
    def name(self) -> str:
        return self._metadata.name

    @override
    @property
    def created_at(self) -> datetime:
        return self._metadata.created_at

    @override
    @property
    def accessed_at(self) -> datetime:
        return self._metadata.accessed_at

    @override
    @property
    def modified_at(self) -> datetime:
        return self._metadata.modified_at

    @override
    @property
    def item_count(self) -> int:
        return self._metadata.item_count

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: str | None = None,
        name: str | None = None,
        storage_dir: Path | None = None,
    ) -> MemoryDatasetClient:
        if storage_dir is not None:
            logger.warning('The `storage_dir` argument is not used in the memory dataset client.')

        name = name or cls._DEFAULT_NAME

        # Check if the client is already cached by name.
        if name in _cache_by_name:
            client = _cache_by_name[name]
            await client._update_metadata(update_accessed_at=True)  # noqa: SLF001
            return client

        dataset_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        client = cls(
            id=dataset_id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
            item_count=0,
        )

        # Cache the client by name
        _cache_by_name[name] = client

        return client

    @override
    async def drop(self) -> None:
        self._records.clear()
        self._metadata.item_count = 0

        # Remove the client from the cache
        if self.name in _cache_by_name:
            del _cache_by_name[self.name]

    @override
    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
        new_item_count = self.item_count

        if isinstance(data, list):
            for item in data:
                new_item_count += 1
                await self._push_item(item)
        else:
            new_item_count += 1
            await self._push_item(data)

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
        # Check for unsupported arguments and log a warning if found
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
                f'The arguments {list(unsupported.keys())} of get_data are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        total = len(self._records)
        items = self._records.copy()

        # Apply skip_empty filter if requested
        if skip_empty:
            items = [item for item in items if item]

        # Apply sorting
        if desc:
            items = list(reversed(items))

        # Apply pagination
        sliced_items = items[offset : (offset + limit) if limit is not None else total]

        await self._update_metadata(update_accessed_at=True)

        return DatasetItemsListPage(
            count=len(sliced_items),
            offset=offset,
            limit=limit or (total - offset),
            total=total,
            desc=desc,
            items=sliced_items,
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
        # Check for unsupported arguments and log a warning if found
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

        items = self._records.copy()

        # Apply sorting
        if desc:
            items = list(reversed(items))

        # Apply pagination
        sliced_items = items[offset : (offset + limit) if limit is not None else len(items)]

        # Yield items one by one
        for item in sliced_items:
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
        """Update the dataset metadata with current information.

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
        if new_item_count:
            self._metadata.item_count = new_item_count

    async def _push_item(self, item: dict[str, Any]) -> None:
        """Push a single item to the dataset.

        Args:
            item: The data item to add to the dataset.
        """
        self._records.append(item)
