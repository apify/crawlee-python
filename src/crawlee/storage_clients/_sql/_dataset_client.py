from __future__ import annotations

import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select
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
        orm_metadata: DatasetMetadataDB,
        storage_client: SQLStorageClient,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlDatasetClient.open` class method to create a new instance.
        """
        self._orm_metadata = orm_metadata
        self._storage_client = storage_client

    def create_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this key-value store."""
        return self._storage_client.create_session()

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return DatasetMetadata.model_validate(self._orm_metadata)

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
            if id:
                orm_metadata = await session.get(DatasetMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Dataset with ID "{id}" not found.')

                client = cls(
                    orm_metadata=orm_metadata,
                    storage_client=storage_client,
                )
                await client._update_metadata(update_accessed_at=True)

            else:
                stmt = select(DatasetMetadataDB).where(DatasetMetadataDB.name == name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()
                if orm_metadata:
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )
                    await client._update_metadata(update_accessed_at=True)

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
                    orm_metadata = DatasetMetadataDB(**metadata.model_dump())
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )
                    session.add(orm_metadata)

            await session.commit()

            return client

    @override
    async def drop(self) -> None:
        async with self.create_session() as session:
            dataset_db = await session.get(DatasetMetadataDB, self._orm_metadata.id)
            if dataset_db:
                await session.delete(dataset_db)
                await session.commit()

    @override
    async def purge(self) -> None:
        async with self.create_session() as session:
            stmt = delete(DatasetItemDB).where(DatasetItemDB.dataset_id == self._orm_metadata.id)
            await session.execute(stmt)

            self._orm_metadata.item_count = 0
            await self._update_metadata(update_accessed_at=True, update_modified_at=True)
            await session.commit()

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if not isinstance(data, list):
            data = [data]

        db_items: list[DatasetItemDB] = []

        for item in data:
            json_item = json.dumps(item, default=str, ensure_ascii=False)
            db_items.append(
                DatasetItemDB(
                    dataset_id=self._orm_metadata.id,
                    data=json_item,
                    created_at=datetime.now(timezone.utc),
                )
            )

        async with self.create_session() as session:
            session.add_all(db_items)
            self._orm_metadata.item_count += len(data)
            await self._update_metadata(
                update_accessed_at=True,
                update_modified_at=True,
            )

            await session.commit()

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

        stmt = select(DatasetItemDB).where(DatasetItemDB.dataset_id == self._orm_metadata.id)

        if skip_empty:
            stmt = stmt.where(DatasetItemDB.data != '"{}"')

        stmt = stmt.order_by(DatasetItemDB.created_at.desc()) if desc else stmt.order_by(DatasetItemDB.created_at.asc())

        stmt = stmt.offset(offset).limit(limit)

        async with self.create_session() as session:
            result = await session.execute(stmt)
            db_items = result.scalars().all()

            await self._update_metadata(update_accessed_at=True)

            await session.commit()

        items = [json.loads(db_item.data) for db_item in db_items]
        return DatasetItemsListPage(
            items=items,
            count=len(items),
            desc=desc,
            limit=limit or 0,
            offset=offset or 0,
            total=self._orm_metadata.item_count,
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

        stmt = select(DatasetItemDB).where(DatasetItemDB.dataset_id == self._orm_metadata.id)

        if skip_empty:
            stmt = stmt.where(DatasetItemDB.data != '"{}"')

        stmt = stmt.order_by(DatasetItemDB.created_at.desc()) if desc else stmt.order_by(DatasetItemDB.created_at.asc())

        stmt = stmt.offset(offset).limit(limit)

        async with self.create_session() as session:
            result = await session.execute(stmt)
            db_items = result.scalars().all()

            await self._update_metadata(update_accessed_at=True)

            await session.commit()

        items = [json.loads(db_item.data) for db_item in db_items]
        for item in items:
            yield item

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> None:
        """Update the KVS metadata in the database.

        Args:
            session: The SQLAlchemy AsyncSession to use for the update.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
        """
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            self._orm_metadata.accessed_at = now
        if update_modified_at:
            self._orm_metadata.modified_at = now
