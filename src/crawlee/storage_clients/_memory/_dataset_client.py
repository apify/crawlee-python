from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

logger = getLogger(__name__)


class MemoryDatasetClient(DatasetClient):
    """A memory implementation of the dataset client.

    This client stores dataset items in memory using a dictionary.
    No data is persisted to the file system.
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
        """Initialize a new instance of the memory-only dataset client.

        Preferably use the `MemoryDatasetClient.open` class method to create a new instance.
        """
        self._id = id
        self._name = name
        self._created_at = created_at
        self._accessed_at = accessed_at
        self._modified_at = modified_at
        self._item_count = item_count

        # Dictionary to hold dataset items; keys are zero-padded strings.
        self._records = list[dict[str, Any]]()

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

    @override
    @classmethod
    async def open(
        cls,
        id: str | None,
        name: str | None,
        storage_dir: Path,  # Ignored in the memory-only implementation.
    ) -> MemoryDatasetClient:
        name = name or cls._DEFAULT_NAME
        dataset_id = id or crypto_random_object_id()
        now = datetime.now(timezone.utc)
        return cls(
            id=dataset_id,
            name=name,
            created_at=now,
            accessed_at=now,
            modified_at=now,
            item_count=0,
        )

    @override
    async def drop(self) -> None:
        self._records.clear()
        self._item_count = 0

    @override
    async def push_data(self, data: list[Any] | dict[str, Any]) -> None:
        if isinstance(data, list):
            for item in data:
                await self._push_item(item)
        else:
            await self._push_item(data)
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

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
        unsupported_args = [clean, fields, omit, unwind, skip_hidden, flatten, view]
        invalid = [arg for arg in unsupported_args if arg not in (False, None)]
        if invalid:
            logger.warning(
                f'The arguments {invalid} of get_data are not supported by the {self.__class__.__name__} client.'
            )

        total = len(self._records)
        items = self._records.copy()
        if desc:
            items = list(reversed(items))

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
        unsupported_args = [clean, fields, omit, unwind, skip_hidden]
        invalid = [arg for arg in unsupported_args if arg not in (False, None)]
        if invalid:
            logger.warning(
                f'The arguments {invalid} of iterate are not supported by the {self.__class__.__name__} client.'
            )

        items = self._records.copy()
        if desc:
            items = list(reversed(items))

        sliced_items = items[offset : (offset + limit) if limit is not None else len(items)]
        for item in sliced_items:
            if skip_empty and not item:
                continue
            yield item

        await self._update_metadata(update_accessed_at=True)

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the dataset metadata file with current information.

        Args:
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)
        if update_accessed_at:
            self._accessed_at = now
        if update_modified_at:
            self._modified_at = now

    async def _push_item(self, item: dict[str, Any]) -> None:
        """Push a single item to the dataset."""
        self._item_count += 1
        self._records.append(item)
