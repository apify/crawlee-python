from __future__ import annotations

import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import CursorResult, delete, select
from sqlalchemy import func as sql_func
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import Self, override

from crawlee._utils.file import infer_mime_type
from crawlee._utils.retry import retry_on_error
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients._utils import batch_records_by_size
from crawlee.storage_clients.models import (
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

from ._client_mixin import MetadataUpdateParams, SqlClientMixin
from ._db_models import KeyValueStoreMetadataBufferDb, KeyValueStoreMetadataDb, KeyValueStoreRecordDb

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class SqlKeyValueStoreClient(KeyValueStoreClient, SqlClientMixin):
    """SQL implementation of the key-value store client.

    This client persists key-value data to a SQL database with transaction support and
    concurrent access safety. Keys are mapped to rows in database tables with proper indexing
    for efficient retrieval.

    The key-value store data is stored in SQL database tables following the pattern:
    - `key_value_stores` table: Contains store metadata (id, name, timestamps)
    - `key_value_store_records` table: Contains individual key-value pairs with binary value storage, content type,
    and size information
    - `key_value_store_metadata_buffer` table: Buffers metadata updates for performance optimization

    Values are serialized based on their type: JSON objects are stored as formatted JSON,
    text values as UTF-8 encoded strings, and binary data as-is in the `LargeBinary` column.
    The implementation automatically handles content type detection and maintains metadata
    about each record including size and MIME type information.

    All database operations are wrapped in transactions with proper error handling and rollback
    mechanisms. The client supports atomic upsert operations and handles race conditions when
    multiple clients access the same store using composite primary keys (key_value_store_id, key).
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _ITERATE_ENTRIES_BATCH_MAX_KEYS = 100
    """Maximum number of records listed or read with a single query in `iterate_entries`."""

    _ITERATE_ENTRIES_BATCH_MAX_BYTES = 8 * 1024 * 1024
    """Maximum total size of the records read with a single query in `iterate_entries`.

    A single record larger than this is still read, but alone in its batch.
    """

    _METADATA_TABLE = KeyValueStoreMetadataDb
    """SQLAlchemy model for key-value store metadata."""

    _ITEM_TABLE = KeyValueStoreRecordDb
    """SQLAlchemy model for key-value store items."""

    _CLIENT_TYPE = 'Key-value store'
    """Human-readable client type for error messages."""

    _BUFFER_TABLE = KeyValueStoreMetadataBufferDb
    """SQLAlchemy model for metadata buffer."""

    def __init__(
        self,
        *,
        storage_client: SqlStorageClient,
        id: str,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SqlKeyValueStoreClient.open` class method to create a new instance.
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
        """Open or create a SQL key-value store client.

        This method attempts to open an existing key-value store from the SQL database. If a KVS with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            storage_client: The SQL storage client used to access the database.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a store with the specified ID is not found, or if metadata is invalid.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            alias=alias,
            storage_client=storage_client,
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
        now = datetime.now(timezone.utc)
        await self._purge(metadata_kwargs=MetadataUpdateParams(accessed_at=now, modified_at=now))

    @retry_on_error(SQLAlchemyError)
    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        # Special handling for None values
        if value is None:
            content_type = 'application/x-none'  # Special content type to identify None values
            value_bytes = b''
        else:
            content_type = content_type or infer_mime_type(value)

            # Serialize the value to bytes.
            if 'application/json' in content_type:
                value_bytes = json.dumps(value, default=str, ensure_ascii=False).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                value_bytes = value
            else:
                # Fallback: attempt to convert to string and encode.
                value_bytes = str(value).encode('utf-8')

        size = len(value_bytes)
        insert_values = {
            'key_value_store_id': self._id,
            'key': key,
            'value': value_bytes,
            'content_type': content_type,
            'size': size,
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

        return self._build_record(
            key=record_db.key,
            content_type=record_db.content_type,
            size=record_db.size,
            value_bytes=record_db.value,
        )

    @staticmethod
    def _build_record(
        *, key: str, content_type: str, size: int | None, value_bytes: bytes
    ) -> KeyValueStoreRecord | None:
        """Deserialize a stored value based on its content type into a record.

        Returns None, after logging a warning, when the stored bytes cannot be decoded as the content type claims.
        """
        # Handle None values
        if content_type == 'application/x-none':
            value = None
        # Handle JSON values
        elif 'application/json' in content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        # Handle text values
        elif content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        # Handle binary values
        else:
            value = value_bytes

        return KeyValueStoreRecord(key=key, value=value, content_type=content_type, size=size)

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

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        # Build query for record metadata
        stmt = (
            select(self._ITEM_TABLE.key, self._ITEM_TABLE.content_type, self._ITEM_TABLE.size)
            .where(self._ITEM_TABLE.key_value_store_id == self._id)
            .order_by(self._ITEM_TABLE.key)
        )

        # Apply exclusive_start_key filter
        if exclusive_start_key is not None:
            stmt = stmt.where(self._ITEM_TABLE.key > exclusive_start_key)

        # Apply limit
        if limit is not None:
            stmt = stmt.limit(limit)

        async with self.get_session(with_simple_commit=True) as session:
            result = await session.stream(stmt.execution_options(stream_results=True))

            async for row in result:
                yield KeyValueStoreRecordMetadata(
                    key=row.key,
                    content_type=row.content_type,
                    size=row.size,
                )

            await self._add_buffer_record(session)

    @override
    async def iterate_entries(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecord]:
        """Iterate over all the existing records in the key-value store, including their values.

        The records are read in keyset-paginated pages of metadata, and the values of each page are then fetched with
        a single query per batch, instead of one query per record as the default implementation does. The batches are
        bounded by the record sizes, so a store with large values does not load too many of them at once. Every query
        runs in its own short session, so no transaction stays open while the consumer processes the records.
        """
        last_key = exclusive_start_key
        remaining = limit

        while remaining is None or remaining > 0:
            page_size = self._ITERATE_ENTRIES_BATCH_MAX_KEYS
            if remaining is not None:
                page_size = min(page_size, remaining)

            page = await self._list_record_metadata(exclusive_start_key=last_key, limit=page_size)
            if not page:
                return

            for batch in batch_records_by_size(
                page,
                max_records=self._ITERATE_ENTRIES_BATCH_MAX_KEYS,
                max_bytes=self._ITERATE_ENTRIES_BATCH_MAX_BYTES,
            ):
                for record in await self._fetch_records(batch):
                    yield record

            last_key = page[-1].key
            if remaining is not None:
                remaining -= len(page)
            if len(page) < page_size:
                return

    @retry_on_error(SQLAlchemyError)
    async def _list_record_metadata(
        self, *, exclusive_start_key: str | None, limit: int
    ) -> list[KeyValueStoreRecordMetadata]:
        """Read one page of record metadata, ordered by key and starting after `exclusive_start_key`."""
        stmt = (
            select(self._ITEM_TABLE.key, self._ITEM_TABLE.content_type, self._ITEM_TABLE.size)
            .where(self._ITEM_TABLE.key_value_store_id == self._id)
            .order_by(self._ITEM_TABLE.key)
            .limit(limit)
        )
        if exclusive_start_key is not None:
            stmt = stmt.where(self._ITEM_TABLE.key > exclusive_start_key)

        async with self.get_session(with_simple_commit=True) as session:
            result = await session.execute(stmt)
            page = [
                KeyValueStoreRecordMetadata(key=row.key, content_type=row.content_type, size=row.size) for row in result
            ]
            await self._add_buffer_record(session)

        return page

    @retry_on_error(SQLAlchemyError)
    async def _fetch_records(self, batch: list[KeyValueStoreRecordMetadata]) -> list[KeyValueStoreRecord]:
        """Fetch the values of the given records with a single query and return the deserialized records."""
        stmt = (
            select(
                self._ITEM_TABLE.key,
                self._ITEM_TABLE.content_type,
                self._ITEM_TABLE.size,
                self._ITEM_TABLE.value,
            )
            .where(
                self._ITEM_TABLE.key_value_store_id == self._id,
                self._ITEM_TABLE.key.in_([item.key for item in batch]),
            )
            .order_by(self._ITEM_TABLE.key)
        )

        async with self.get_session(with_simple_commit=True) as session:
            result = await session.execute(stmt)
            rows = result.all()

        records = (
            self._build_record(key=row.key, content_type=row.content_type, size=row.size, value_bytes=row.value)
            for row in rows
        )
        return [record for record in records if record is not None]

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
