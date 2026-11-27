from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self, cast

from sqlalchemy import CursorResult, delete, select
from sqlalchemy import func as sql_func
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import override

from crawlee._utils.retry import retry_on_error
from crawlee.storage_backends._base import KeyValueStoreBackend
from crawlee.storage_backends.models import (
    KeyValueStoreKeysListPage,
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

from ._backend_mixin import MetadataUpdateParams, SqlBackendMixin
from ._db_models import KeyValueStoreMetadataBufferDb, KeyValueStoreMetadataDb, KeyValueStoreRecordDb

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_backend import SqlStorageBackend


class SqlKeyValueStoreBackend(
    KeyValueStoreBackend, SqlBackendMixin[KeyValueStoreMetadataDb, KeyValueStoreMetadataBufferDb, KeyValueStoreRecordDb]
):
    """SQL implementation of the key-value store backend.

    This backend persists key-value data to a SQL database with transaction support and
    concurrent access safety. Keys are mapped to rows in database tables with proper indexing
    for efficient retrieval.

    The key-value store data is stored in SQL database tables following the pattern:
    - `key_value_stores` table: Contains store metadata (id, name, timestamps)
    - `key_value_store_records` table: Contains individual key-value pairs with binary value storage, content type,
    and size information
    - `key_value_store_metadata_buffer` table: Buffers metadata updates for performance optimization

    The values are stored as the bytes `KeyValueStore` serializes them to, in the `LargeBinary` column, together with
    the content type and the size of each record.

    All database operations are wrapped in transactions with proper error handling and rollback
    mechanisms. The backend supports atomic upsert operations and handles race conditions when
    multiple clients access the same store using composite primary keys (key_value_store_id, key).
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _METADATA_TABLE = KeyValueStoreMetadataDb
    """SQLAlchemy model for key-value store metadata."""

    _ITEM_TABLE = KeyValueStoreRecordDb
    """SQLAlchemy model for key-value store items."""

    _STORAGE_TYPE = 'Key-value store'
    """Human-readable storage type for error messages."""

    _BUFFER_TABLE = KeyValueStoreMetadataBufferDb
    """SQLAlchemy model for metadata buffer."""

    def __init__(
        self,
        *,
        storage_backend: SqlStorageBackend,
        id: str,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlKeyValueStoreBackend.open` class method to create a new instance.
        """
        super().__init__(id=id, storage_backend=storage_backend)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        storage_backend: SqlStorageBackend,
    ) -> Self:
        """Open or create a SQL key-value store backend.

        This method attempts to open an existing key-value store from the SQL database. If a KVS with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            storage_backend: The SQL storage backend used to access the database.

        Returns:
            An instance for the opened or created storage backend.

        Raises:
            ValueError: If a store with the specified ID is not found, or if metadata is invalid.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            alias=alias,
            storage_backend=storage_backend,
            metadata_model=KeyValueStoreMetadata,
            extra_metadata_fields={},
        )

    @retry_on_error(SQLAlchemyError)
    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        # The database is a single place of truth
        return await self._get_metadata(KeyValueStoreMetadata)

    @retry_on_error(SQLAlchemyError)
    @override
    async def drop(self) -> None:
        """Delete this key-value store and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        await self._drop()

    @retry_on_error(SQLAlchemyError)
    @override
    async def purge(self) -> None:
        """Remove all items from this key-value store while keeping the key-value store structure.

        Remove all records from key_value_store_records table.
        """
        now = datetime.now(UTC)
        await self._purge(metadata_kwargs=MetadataUpdateParams(accessed_at=now, modified_at=now))

    @retry_on_error(SQLAlchemyError)
    @override
    async def set_value(self, *, key: str, value: bytes, content_type: str) -> None:
        insert_values = {
            'key_value_store_id': self._id,
            'key': key,
            'value': value,
            'content_type': content_type,
            'size': len(value),
        }

        upsert_stmt = self._build_upsert_stmt(
            self._ITEM_TABLE,
            insert_values=insert_values,
            update_columns=['value', 'content_type', 'size'],
            conflict_cols=['key_value_store_id', 'key'],
        )

        async with self.get_session(with_simple_commit=True) as session:
            await session.execute(upsert_stmt)

            await self._add_buffer_record(session, update_modified_at=True)

    @retry_on_error(SQLAlchemyError)
    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # Query the record by key
        stmt = select(self._ITEM_TABLE).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session(with_simple_commit=True) as session:
            result = await session.execute(stmt)
            record_db = result.scalar_one_or_none()

            await self._add_buffer_record(session)

        if not record_db:
            return None

        return KeyValueStoreRecord(
            key=record_db.key,
            value=record_db.value,
            content_type=record_db.content_type,
            size=record_db.size,
        )

    @retry_on_error(SQLAlchemyError)
    @override
    async def get_values(self, *, keys: Sequence[str]) -> Mapping[str, KeyValueStoreRecord]:
        stmt = select(
            self._ITEM_TABLE.key,
            self._ITEM_TABLE.content_type,
            self._ITEM_TABLE.size,
            self._ITEM_TABLE.value,
        ).where(
            self._ITEM_TABLE.key_value_store_id == self._id,
            self._ITEM_TABLE.key.in_(keys),
        )

        async with self.get_session(with_simple_commit=True) as session:
            result = await session.execute(stmt)
            rows = result.all()

            await self._add_buffer_record(session)

        return {
            row.key: KeyValueStoreRecord(key=row.key, value=row.value, content_type=row.content_type, size=row.size)
            for row in rows
        }

    @retry_on_error(SQLAlchemyError)
    @override
    async def delete_value(self, *, key: str) -> None:
        stmt = delete(self._ITEM_TABLE).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session(with_simple_commit=True) as session:
            # Delete the record if it exists
            result = await session.execute(stmt)
            result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result

            # Update metadata if we actually deleted something
            if result.rowcount > 0:
                await self._add_buffer_record(session, update_modified_at=True)

    @retry_on_error(SQLAlchemyError)
    @override
    async def list_keys(
        self,
        *,
        prefix: str | None = None,
        exclusive_start_key: str | None = None,
        limit: int = 1000,
    ) -> KeyValueStoreKeysListPage:
        # One row over the limit shows whether more keys follow.
        stmt = (
            select(self._ITEM_TABLE.key, self._ITEM_TABLE.content_type, self._ITEM_TABLE.size)
            .where(self._ITEM_TABLE.key_value_store_id == self._id)
            .order_by(self._ITEM_TABLE.key)
            .limit(limit + 1)
        )

        if prefix is not None:
            # The lower bound lets the database start the scan at the prefix instead of at the first key.
            stmt = stmt.where(
                self._ITEM_TABLE.key >= prefix,
                self._ITEM_TABLE.key.startswith(prefix, autoescape=True),
            )

        if exclusive_start_key is not None:
            stmt = stmt.where(self._ITEM_TABLE.key > exclusive_start_key)

        async with self.get_session(with_simple_commit=True) as session:
            result = await session.execute(stmt)
            rows = result.all()

            await self._add_buffer_record(session)

        is_truncated = len(rows) > limit
        rows = rows[:limit]

        # `LIKE` ignores the case in some databases, so the prefix is checked again here. A page can then hold fewer
        # keys than the limit, while the next page still starts after the last row the database returned.
        items = [
            KeyValueStoreRecordMetadata(key=row.key, content_type=row.content_type, size=row.size)
            for row in rows
            if prefix is None or row.key.startswith(prefix)
        ]

        return KeyValueStoreKeysListPage(
            items=items,
            count=len(items),
            limit=limit,
            exclusive_start_key=exclusive_start_key,
            next_exclusive_start_key=rows[-1].key if is_truncated else None,
        )

    @retry_on_error(SQLAlchemyError)
    @override
    async def record_exists(self, *, key: str) -> bool:
        stmt = select(self._ITEM_TABLE.key).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session(with_simple_commit=True) as session:
            # Check if record exists
            result = await session.execute(stmt)

            await self._add_buffer_record(session)

            return result.scalar_one_or_none() is not None

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for SQL key-value stores.')

    @override
    def _specific_update_metadata(self, **_kwargs: dict[str, Any]) -> dict[str, Any]:
        return {}

    @override
    def _prepare_buffer_data(self, **_kwargs: Any) -> dict[str, Any]:
        """Prepare key-value store specific buffer data.

        For KeyValueStore, we don't have specific metadata fields to track in buffer,
        so we just return empty dict. The base buffer will handle accessed_at/modified_at.
        """
        return {}

    @override
    async def _apply_buffer_updates(self, session: AsyncSession, max_buffer_id: int) -> None:
        aggregation_stmt = select(
            sql_func.max(self._BUFFER_TABLE.accessed_at).label('max_accessed_at'),
            sql_func.max(self._BUFFER_TABLE.modified_at).label('max_modified_at'),
        ).where(self._BUFFER_TABLE.storage_id == self._id, self._BUFFER_TABLE.id <= max_buffer_id)

        result = await session.execute(aggregation_stmt)
        row = result.first()

        if not row:
            return

        await self._update_metadata(
            session,
            **MetadataUpdateParams(
                accessed_at=row.max_accessed_at,
                modified_at=row.max_modified_at,
            ),
        )
