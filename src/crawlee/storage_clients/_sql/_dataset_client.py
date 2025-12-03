from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import Select, insert, select
from typing_extensions import Self, override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import MetadataUpdateParams, SqlClientMixin
from ._db_models import DatasetItemDb, DatasetMetadataDb

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Select
    from typing_extensions import NotRequired

    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class _DatasetMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating dataset metadata."""

    new_item_count: NotRequired[int]
    delta_item_count: NotRequired[int]


class SqlDatasetClient(DatasetClient, SqlClientMixin):
    """SQL implementation of the dataset client.

    This client persists dataset items to a SQL database using two tables for storage
    and retrieval. Items are stored as JSON with automatic ordering preservation.

    The dataset data is stored in SQL database tables following the pattern:
    - `datasets` table: Contains dataset metadata (id, name, timestamps, item_count)
    - `dataset_records` table: Contains individual items with JSON data and auto-increment ordering

    Items are stored as a JSON object in SQLite and as JSONB in PostgreSQL. These objects must be JSON-serializable.
    The `item_id` auto-increment primary key ensures insertion order is preserved.
    All operations are wrapped in database transactions with CASCADE deletion support.
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _METADATA_TABLE = DatasetMetadataDb
    """SQLAlchemy model for dataset metadata."""

    _ITEM_TABLE = DatasetItemDb
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

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        storage_client: SqlStorageClient,
    ) -> Self:
        """Open an existing dataset or create a new one.

        Args:
            id: The ID of the dataset to open. If provided, searches for existing dataset by ID.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            storage_client: The SQL storage client instance.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a dataset with the specified ID is not found.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            alias=alias,
            storage_client=storage_client,
            metadata_model=DatasetMetadata,
            extra_metadata_fields={'item_count': 0},
        )

    @override
    async def get_metadata(self) -> DatasetMetadata:
        # The database is a single place of truth
        return await self._get_metadata(DatasetMetadata)

    @override
    async def drop(self) -> None:
        """Delete this dataset and all its items from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related items.
        """
        await self._drop()

    @override
    async def purge(self) -> None:
        """Remove all items from this dataset while keeping the dataset structure.

        Resets item_count to 0 and deletes all records from dataset_records table.
        """
        await self._purge(
            metadata_kwargs=_DatasetMetadataUpdateParams(
                new_item_count=0,
                update_accessed_at=True,
                update_modified_at=True,
                force=True,
            )
        )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if not isinstance(data, list):
            data = [data]

        db_items: list[dict[str, Any]] = []
        db_items = [{'dataset_id': self._id, 'data': item} for item in data]
        stmt = insert(self._ITEM_TABLE).values(db_items)

        async with self.get_session(with_simple_commit=True) as session:
            await session.execute(stmt)

            await self._update_metadata(
                session,
                **_DatasetMetadataUpdateParams(
                    update_accessed_at=True,
                    update_modified_at=True,
                    delta_item_count=len(data),
                    new_item_count=len(data),
                    force=True,
                ),
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

            updated = await self._update_metadata(session, **_DatasetMetadataUpdateParams(update_accessed_at=True))

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

            updated = await self._update_metadata(session, **_DatasetMetadataUpdateParams(update_accessed_at=True))

            # Commit updates to the metadata
            if updated:
                await session.commit()

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

        stmt = select(self._ITEM_TABLE).where(self._ITEM_TABLE.dataset_id == self._id)

        if skip_empty:
            # Skip items that are empty JSON objects
            stmt = stmt.where(self._ITEM_TABLE.data != {})

        # Apply ordering by insertion order (item_id)
        stmt = stmt.order_by(self._ITEM_TABLE.item_id.desc()) if desc else stmt.order_by(self._ITEM_TABLE.item_id.asc())

        return stmt.offset(offset).limit(limit)

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
