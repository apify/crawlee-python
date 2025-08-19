from __future__ import annotations

import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import Select, delete, insert, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import SQLClientMixin
from ._db_models import DatasetItemDB, DatasetMetadataDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Select
    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient

logger = getLogger(__name__)


class SQLDatasetClient(DatasetClient, SQLClientMixin):
    """SQL implementation of the dataset client.

    This client persists dataset items to a SQL database using two tables for storage
    and retrieval. Items are stored as JSON with automatic ordering preservation.

    The dataset data is stored in SQL database tables following the pattern:
    - `dataset_metadata` table: Contains dataset metadata (id, name, timestamps, item_count)
    - `dataset_item` table: Contains individual items with JSON data and auto-increment ordering

    Items are serialized to JSON with `default=str` to handle non-serializable types like datetime
    objects. The `order_id` auto-increment primary key ensures insertion order is preserved.
    All operations are wrapped in database transactions with CASCADE deletion support.
    """

    _DEFAULT_NAME_DB = 'default'
    """Default dataset name used when no name is provided."""

    def __init__(
        self,
        *,
        id: str,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlDatasetClient.open` class method to create a new instance.
        """
        self._id = id
        self._storage_client = storage_client

        # Time tracking to reduce database writes during frequent operation
        self._last_accessed_at: datetime | None = None
        self._last_modified_at: datetime | None = None
        self._accessed_modified_update_interval = storage_client.get_accessed_modified_update_interval()

    @override
    async def get_metadata(self) -> DatasetMetadata:
        """Get dataset metadata from the database."""
        # The database is a single place of truth
        async with self.get_session() as session:
            orm_metadata: DatasetMetadataDB | None = await session.get(DatasetMetadataDB, self._id)
            if not orm_metadata:
                raise ValueError(f'Dataset with ID "{self._id}" not found.')

            return DatasetMetadata.model_validate(orm_metadata)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SQLStorageClient,
    ) -> SQLDatasetClient:
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
        async with storage_client.create_session() as session:
            orm_metadata: DatasetMetadataDB | None = None
            if id:
                orm_metadata = await session.get(DatasetMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Dataset with ID "{id}" not found.')
            else:
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(DatasetMetadataDB).where(DatasetMetadataDB.name == search_name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()

            if orm_metadata:
                client = cls(
                    id=orm_metadata.id,
                    storage_client=storage_client,
                )
                await client._update_metadata(session, update_accessed_at=True)
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
                    id=metadata.id,
                    storage_client=storage_client,
                )
                session.add(DatasetMetadataDB(**metadata.model_dump()))

            try:
                # Commit the insert or update metadata to the database
                await session.commit()
            except SQLAlchemyError:
                # Attempt to open simultaneously by different clients.
                # The commit that created the record has already been executed, make rollback and get by name.
                await session.rollback()
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(DatasetMetadataDB).where(DatasetMetadataDB.name == search_name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()
                if not orm_metadata:
                    raise ValueError(f'Dataset with Name "{search_name}" not found.') from None
                client = cls(
                    id=orm_metadata.id,
                    storage_client=storage_client,
                )

            return client

    @override
    async def drop(self) -> None:
        """Delete this dataset and all its items from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related items.
        """
        stmt = delete(DatasetMetadataDB).where(DatasetMetadataDB.id == self._id)
        async with self.get_autocommit_session() as autocommit:
            if self._storage_client.get_dialect_name() == 'sqlite':
                # foreign_keys=ON is set at the connection level. Required for cascade deletion.
                await autocommit.execute(text('PRAGMA foreign_keys=ON'))
            await autocommit.execute(stmt)

    @override
    async def purge(self) -> None:
        """Remove all items from this dataset while keeping the dataset structure.

        Resets item_count to 0 and deletes all records from dataset_item table.
        """
        stmt = delete(DatasetItemDB).where(DatasetItemDB.dataset_id == self._id)
        async with self.get_autocommit_session() as autocommit:
            await autocommit.execute(stmt)

            await self._update_metadata(autocommit, new_item_count=0, update_accessed_at=True, update_modified_at=True)

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        """Add new items to the dataset."""
        if not isinstance(data, list):
            data = [data]

        db_items: list[dict[str, Any]] = []

        for item in data:
            # Serialize with default=str to handle non-serializable types like datetime
            json_item = json.dumps(item, default=str, ensure_ascii=False)
            db_items.append(
                {
                    'dataset_id': self._id,
                    'data': json_item,
                }
            )

        stmt = insert(DatasetItemDB).values(db_items)

        async with self.get_autocommit_session() as autocommit:
            await autocommit.execute(stmt)

            await self._update_metadata(
                autocommit, update_accessed_at=True, update_modified_at=True, delta_item_count=len(data)
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
        unwind: str | None = None,
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

        stmt = select(DatasetItemDB).where(DatasetItemDB.dataset_id == self._id)

        if skip_empty:
            # Skip items that are empty JSON objects
            stmt = stmt.where(DatasetItemDB.data != '"{}"')

        # Apply ordering by insertion order (order_id)
        stmt = stmt.order_by(DatasetItemDB.order_id.desc()) if desc else stmt.order_by(DatasetItemDB.order_id.asc())

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
        unwind: str | None = None,
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

        # Deserialize JSON items
        items = [json.loads(db_item.data) for db_item in db_items]
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
        unwind: str | None = None,
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
                yield json.loads(db_item.data)

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        new_item_count: int | None = None,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        delta_item_count: int | None = None,
    ) -> bool:
        """Update the dataset metadata in the database.

        Args:
            session: The SQLAlchemy AsyncSession to use for the update.
            new_item_count: If provided, set item count to this value.
            update_accessed_at: If True, update the accessed_at timestamp.
            update_modified_at: If True, update the modified_at timestamp.
            delta_item_count: If provided, add this value to the current item count.
        """
        now = datetime.now(timezone.utc)
        values_to_set: dict[str, Any] = {}

        if update_accessed_at and (
            self._last_accessed_at is None or (now - self._last_accessed_at) > self._accessed_modified_update_interval
        ):
            values_to_set['accessed_at'] = now
            self._last_accessed_at = now

        if update_modified_at and (
            self._last_modified_at is None or (now - self._last_modified_at) > self._accessed_modified_update_interval
        ):
            values_to_set['modified_at'] = now
            self._last_modified_at = now

        if new_item_count is not None:
            values_to_set['item_count'] = new_item_count
        elif delta_item_count:
            # Use database-level for atomic updates
            values_to_set['item_count'] = DatasetMetadataDB.item_count + delta_item_count

        if values_to_set:
            stmt = update(DatasetMetadataDB).where(DatasetMetadataDB.id == self._id).values(**values_to_set)
            await session.execute(stmt)
            return True

        return False
