from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import Select, insert, select
from typing_extensions import override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import SQLClientMixin
from ._db_models import DatasetItemDB, DatasetMetadataDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Select

    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class SqlDatasetClient(DatasetClient, SQLClientMixin):
    """SQL implementation of the dataset client.

    This client persists dataset items to a SQL database using two tables for storage
    and retrieval. Items are stored as JSON with automatic ordering preservation.

    The dataset data is stored in SQL database tables following the pattern:
    - `dataset_metadata` table: Contains dataset metadata (id, name, timestamps, item_count)
    - `dataset_item` table: Contains individual items with JSON data and auto-increment ordering

    Items are stored as a JSON object in SQLite and as JSONB in PostgreSQL. These objects must be JSON-serializable.
    The `order_id` auto-increment primary key ensures insertion order is preserved.
    All operations are wrapped in database transactions with CASCADE deletion support.
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _METADATA_TABLE = DatasetMetadataDB
    """SQLAlchemy model for dataset metadata."""

    _ITEM_TABLE = DatasetItemDB
    """SQLAlchemy model for dataset items."""

    _CLIENT_TYPE = 'Dataset'
    """Human-readable client type for error messages."""

    def __init__(
        self,
        *,
        id: str,
        storage_client: SqlStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlDatasetClient.open` class method to create a new instance.
        """
        super().__init__(id=id, storage_client=storage_client)

    @override
    async def get_metadata(self) -> DatasetMetadata:
        """Get dataset metadata from the database."""
        # The database is a single place of truth
        metadata = await self._get_metadata(DatasetMetadata)
        return cast('DatasetMetadata', metadata)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SqlStorageClient,
    ) -> SqlDatasetClient:
        """Open an existing dataset or create a new one.

        Args:
            id: The ID of the dataset to open. If provided, searches for existing dataset by ID.
            name: The name of the dataset to open. If not provided, uses the default dataset.
            storage_client: The SQL storage client instance.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a dataset with the specified ID is not found.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            storage_client=storage_client,
            metadata_model=DatasetMetadata,
            extra_metadata_fields={'itemCount': 0},
        )

    @override
    async def drop(self) -> None:
        """Delete this dataset and all its items from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related items.
        """
        await self._drop()

    @override
    async def purge(self) -> None:
        """Remove all items from this dataset while keeping the dataset structure.

        Resets item_count to 0 and deletes all records from dataset_item table.
        """
        await self._purge(
            metadata_kwargs={'new_item_count': 0, 'update_accessed_at': True, 'update_modified_at': True, 'force': True}
        )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        """Add new items to the dataset."""
        if not isinstance(data, list):
            data = [data]

        db_items: list[dict[str, Any]] = []
        db_items = [{'metadata_id': self._id, 'data': item} for item in data]
        stmt = insert(self._ITEM_TABLE).values(db_items)

        async with self.get_autocommit_session() as autocommit:
            await autocommit.execute(stmt)

            await self._update_metadata(
                autocommit, update_accessed_at=True, update_modified_at=True, delta_item_count=len(data), force=True
            )

    def _prepare_get_stmt(
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
    ) -> Select:
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

        stmt = select(self._ITEM_TABLE).where(self._ITEM_TABLE.metadata_id == self._id)

        if skip_empty:
            # Skip items that are empty JSON objects
            stmt = stmt.where(self._ITEM_TABLE.data != {})

        # Apply ordering by insertion order (order_id)
        stmt = (
            stmt.order_by(self._ITEM_TABLE.order_id.desc()) if desc else stmt.order_by(self._ITEM_TABLE.order_id.asc())
        )

        return stmt.offset(offset).limit(limit)

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
        stmt = self._prepare_get_stmt(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
            flatten=flatten,
            view=view,
        )

        async with self.get_session() as session:
            result = await session.execute(stmt)
            db_items = result.scalars().all()

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

        items = [db_item.data for db_item in db_items]
        metadata = await self.get_metadata()
        return DatasetItemsListPage(
            items=items,
            count=len(items),
            desc=desc,
            limit=limit or 0,
            offset=offset or 0,
            total=metadata.item_count,
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
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate over dataset items with optional filtering and ordering."""
        stmt = self._prepare_get_stmt(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
        )

        async with self.get_session() as session:
            db_items = await session.stream_scalars(stmt)

            async for db_item in db_items:
                yield db_item.data

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

    def _specific_update_metadata(
        self,
        new_item_count: int | None = None,
        delta_item_count: int | None = None,
        **_kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Update the dataset metadata in the database.

        Args:
            session: The SQLAlchemy AsyncSession to use for the update.
            new_item_count: If provided, set item count to this value.
            delta_item_count: If provided, add this value to the current item count.
        """
        values_to_set: dict[str, Any] = {}

        if new_item_count is not None:
            values_to_set['item_count'] = new_item_count
        elif delta_item_count:
            # Use database-level for atomic updates
            values_to_set['item_count'] = self._METADATA_TABLE.item_count + delta_item_count

        return values_to_set
