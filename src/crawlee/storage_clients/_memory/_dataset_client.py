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

logger = getLogger(__name__)


class MemoryDatasetClient(DatasetClient):
    """Memory implementation of the dataset client.

    This client stores dataset items in memory using Python lists and dictionaries. No data is persisted
    between process runs, meaning all stored data is lost when the program terminates. This implementation
    is primarily useful for testing, development, and short-lived crawler operations where persistent
    storage is not required.

    The memory implementation provides fast access to data but is limited by available memory and
    does not support data sharing across different processes. It supports all dataset operations including
    sorting, filtering, and pagination, but performs them entirely in memory.
    """

    def __init__(
        self,
        *,
        metadata: DatasetMetadata,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        self._metadata = metadata

        self._records = list[dict[str, Any]]()
        """List to hold dataset items. Each item is a dictionary representing a record."""

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return self._metadata

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
    ) -> MemoryDatasetClient:
        """Open or create a new memory dataset client.

        This method creates a new in-memory dataset instance. Unlike persistent storage implementations, memory
        datasets don't check for existing datasets with the same name or ID since all data exists only in memory
        and is lost when the process terminates.

        Args:
            id: The ID of the dataset. If not provided, a random ID will be generated.
            name: The name of the dataset. If not provided, the dataset will be unnamed.

        Returns:
            An instance for the opened or created storage client.
        """
        # Otherwise create a new dataset
        dataset_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)

        metadata = DatasetMetadata(
            id=dataset_id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
            item_count=0,
        )

        return cls(metadata=metadata)

    @override
    async def drop(self) -> None:
        self._records.clear()
        await self._update_metadata(
            update_accessed_at=True,
            update_modified_at=True,
            new_item_count=0,
        )

    @override
    async def purge(self) -> None:
        self._records.clear()
        await self._update_metadata(
            update_accessed_at=True,
            update_modified_at=True,
            new_item_count=0,
        )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        metadata = await self.get_metadata()
        new_item_count = metadata.item_count

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
    ) -> AsyncIterator[dict[str, Any]]:
        # Check for unsupported arguments and log a warning if found
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
        if new_item_count is not None:
            self._metadata.item_count = new_item_count

    async def _push_item(self, item: dict[str, Any]) -> None:
        """Push a single item to the dataset.

        Args:
            item: The data item to add to the dataset.
        """
        self._records.append(item)
