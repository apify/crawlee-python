from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, update
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._db_models import DatasetItemDB, DatasetMetadataDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient

logger = getLogger(__name__)


class SQLDatasetClient(DatasetClient):
    """SQL implementation of the dataset client.

    This client persists dataset items to a SQL database with proper transaction handling and
    concurrent access safety. Items are stored in a normalized table structure with automatic
    ordering preservation and efficient querying capabilities.

    The SQL implementation provides ACID compliance, supports complex queries, and allows
    multiple processes to safely access the same dataset concurrently through database-level
    locking mechanisms.
    """

    def __init__(
        self,
        *,
        metadata: DatasetMetadata,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlDatasetClient.open` class method to create a new instance.
        """
        self._metadata = metadata
        self._storage_client = storage_client

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return self._metadata

    def get_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this dataset."""
        return self._storage_client.create_session()

    @asynccontextmanager
    async def get_autocommit_session(self) -> AsyncIterator[AsyncSession]:
        """Create a new SQLAlchemy autocommit session to insert, delete, or modify data."""
        async with self.get_session() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                logger.warning(f'Error occurred during session transaction: {e}')
                await session.rollback()

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SQLStorageClient,
    ) -> SQLDatasetClient:
        """Open or create a SQL dataset client.

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
                stmt = select(DatasetMetadataDB).where(DatasetMetadataDB.name == name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()

            if orm_metadata:
                client = cls(
                    metadata=DatasetMetadata.model_validate(orm_metadata),
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
                    metadata=metadata,
                    storage_client=storage_client,
                )
                session.add(DatasetMetadataDB(**metadata.model_dump()))

            # Commit the insert or update metadata to the database
            await session.commit()

            return client

    @override
    async def drop(self) -> None:
        stmt = delete(DatasetMetadataDB).where(DatasetMetadataDB.id == self._metadata.id)
        async with self.get_autocommit_session() as autocommit:
            await autocommit.execute(stmt)

    @override
    async def purge(self) -> None:
        stmt = delete(DatasetItemDB).where(DatasetItemDB.dataset_id == self._metadata.id)
        async with self.get_autocommit_session() as autocommit:
            await autocommit.execute(stmt)

            await self._update_metadata(autocommit, new_item_count=0, update_accessed_at=True, update_modified_at=True)

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if not isinstance(data, list):
            data = [data]

        db_items: list[DatasetItemDB] = []

        for item in data:
            json_item = json.dumps(item, default=str, ensure_ascii=False)
            db_items.append(
                DatasetItemDB(
                    dataset_id=self._metadata.id,
                    data=json_item,
                )
            )

        async with self.get_autocommit_session() as autocommit:
            autocommit.add_all(db_items)
            await self._update_metadata(
                autocommit, update_accessed_at=True, update_modified_at=True, delta_item_count=len(data)
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

        stmt = select(DatasetItemDB).where(DatasetItemDB.dataset_id == self._metadata.id)

        if skip_empty:
            stmt = stmt.where(DatasetItemDB.data != '"{}"')

        stmt = stmt.order_by(DatasetItemDB.order_id.desc()) if desc else stmt.order_by(DatasetItemDB.order_id.asc())

        stmt = stmt.offset(offset).limit(limit)

        async with self.get_session() as session:
            result = await session.execute(stmt)
            db_items = result.scalars().all()

            await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            await session.commit()

        items = [json.loads(db_item.data) for db_item in db_items]
        return DatasetItemsListPage(
            items=items,
            count=len(items),
            desc=desc,
            limit=limit or 0,
            offset=offset or 0,
            total=self._metadata.item_count,
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

        stmt = select(DatasetItemDB).where(DatasetItemDB.dataset_id == self._metadata.id)

        if skip_empty:
            stmt = stmt.where(DatasetItemDB.data != '"{}"')

        stmt = stmt.order_by(DatasetItemDB.order_id.desc()) if desc else stmt.order_by(DatasetItemDB.order_id.asc())

        stmt = stmt.offset(offset).limit(limit)

        async with self.get_session() as session:
            result = await session.execute(stmt)
            db_items = result.scalars().all()

            await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            await session.commit()

        items = [json.loads(db_item.data) for db_item in db_items]
        for item in items:
            yield item

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        new_item_count: int | None = None,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        delta_item_count: int | None = None,
    ) -> None:
        """Update the KVS metadata in the database.

        Args:
            session: The SQLAlchemy AsyncSession to use for the update.
            new_item_count: If provided, update the item count to this value.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
            delta_item_count: If provided, increment the item count by this value.
        """
        now = datetime.now(timezone.utc)
        values_to_set: dict[str, Any] = {}

        if update_accessed_at:
            self._metadata.accessed_at = now
            values_to_set['accessed_at'] = now
        if update_modified_at:
            self._metadata.modified_at = now
            values_to_set['modified_at'] = now

        if new_item_count is not None:
            self._metadata.item_count = new_item_count
            values_to_set['item_count'] = new_item_count

        if delta_item_count:
            self._metadata.item_count += delta_item_count
            values_to_set['item_count'] = DatasetMetadataDB.item_count + self._metadata.item_count

        if values_to_set:
            stmt = update(DatasetMetadataDB).where(DatasetMetadataDB.id == self._metadata.id).values(**values_to_set)
            await session.execute(stmt)
