from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError
from typing_extensions import override

from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._client_mixin import SQLClientMixin
from ._db_models import KeyValueStoreMetadataDB, KeyValueStoreRecordDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class SqlKeyValueStoreClient(KeyValueStoreClient, SQLClientMixin):
    """SQL implementation of the key-value store client.

    This client persists key-value data to a SQL database with transaction support and
    concurrent access safety. Keys are mapped to rows in database tables with proper indexing
    for efficient retrieval.

    The key-value store data is stored in SQL database tables following the pattern:
    - `kvs_metadata` table: Contains store metadata (id, name, timestamps)
    - `kvs_record` table: Contains individual key-value pairs with binary value storage, content type, and size
        information

    Values are serialized based on their type: JSON objects are stored as formatted JSON,
    text values as UTF-8 encoded strings, and binary data as-is in the `LargeBinary` column.
    The implementation automatically handles content type detection and maintains metadata
    about each record including size and MIME type information.

    All database operations are wrapped in transactions with proper error handling and rollback
    mechanisms. The client supports atomic upsert operations and handles race conditions when
    multiple clients access the same store using composite primary keys (metadata_id, key).
    """

    _DEFAULT_NAME = 'default'
    """Default dataset name used when no name is provided."""

    _METADATA_TABLE = KeyValueStoreMetadataDB
    """SQLAlchemy model for key-value store metadata."""

    _ITEM_TABLE = KeyValueStoreRecordDB
    """SQLAlchemy model for key-value store items."""

    _CLIENT_TYPE = 'Key-value store'
    """Human-readable client type for error messages."""

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

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        """Get the metadata for this key-value store."""
        # The database is a single place of truth
        metadata = await self._get_metadata(KeyValueStoreMetadata)
        return cast('KeyValueStoreMetadata', metadata)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SqlStorageClient,
    ) -> SqlKeyValueStoreClient:
        """Open or create a SQL key-value store client.

        This method attempts to open an existing key-value store from the SQL database. If a KVS with the specified
        ID or name exists, it loads the metadata from the database. If no existing store is found, a new one
        is created.

        Args:
            id: The ID of the key-value store to open. If provided, searches for existing store by ID.
            name: The name of the key-value store to open. If not provided, uses the default store.
            storage_client: The SQL storage client used to access the database.

        Returns:
            An instance for the opened or created storage client.

        Raises:
            ValueError: If a store with the specified ID is not found, or if metadata is invalid.
        """
        return await cls._safely_open(
            id=id,
            name=name,
            storage_client=storage_client,
            metadata_model=KeyValueStoreMetadata,
            extra_metadata_fields={},
        )

    @override
    async def drop(self) -> None:
        """Delete this key-value store and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        await self._drop()

    @override
    async def purge(self) -> None:
        """Remove all items from this key-value store while keeping the key-value store structure."""
        await self._purge(metadata_kwargs={'update_accessed_at': True, 'update_modified_at': True})

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        """Set a value in the key-value store."""
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
            'metadata_id': self._id,
            'key': key,
            'value': value_bytes,
            'content_type': content_type,
            'size': size,
        }
        try:
            # Trying to build a statement for Upsert
            upsert_stmt = self.build_upsert_stmt(
                self._ITEM_TABLE,
                insert_values=insert_values,
                update_columns=['value', 'content_type', 'size'],
                conflict_cols=['metadata_id', 'key'],
            )
        except NotImplementedError:
            # If it is not possible to build an upsert for the current dialect, build an update + insert.
            upsert_stmt = None
            update_stmt = (
                update(self._ITEM_TABLE)
                .where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.key == key)
                .values(value=value_bytes, content_type=content_type, size=size)
            )
            insert_stmt = insert(self._ITEM_TABLE).values(**insert_values)

        async with self.get_session() as session:
            if upsert_stmt is not None:
                result = await session.execute(upsert_stmt)
            else:
                result = await session.execute(update_stmt)
                if result.rowcount == 0:
                    await session.execute(insert_stmt)

            await self._update_metadata(session, update_accessed_at=True, update_modified_at=True)

            try:
                await session.commit()
            except IntegrityError:
                # Race condition when attempting to INSERT the same key. Ignore duplicates.
                await session.rollback()

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        """Get a value from the key-value store."""
        # Query the record by key
        stmt = select(self._ITEM_TABLE).where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.key == key)
        async with self.get_session() as session:
            result = await session.execute(stmt)
            record_db = result.scalar_one_or_none()

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

        if not record_db:
            return None

        # Deserialize the value based on content type
        value_bytes = record_db.value

        # Handle None values
        if record_db.content_type == 'application/x-none':
            value = None
        # Handle JSON values
        elif 'application/json' in record_db.content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        # Handle text values
        elif record_db.content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        # Handle binary values
        else:
            value = value_bytes

        return KeyValueStoreRecord(
            key=record_db.key,
            value=value,
            content_type=record_db.content_type,
            size=record_db.size,
        )

    @override
    async def delete_value(self, *, key: str) -> None:
        """Delete a value from the key-value store."""
        stmt = delete(self._ITEM_TABLE).where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.key == key)
        async with self.get_autocommit_session() as autocommit:
            # Delete the record if it exists
            result = await autocommit.execute(stmt)

            # Update metadata if we actually deleted something
            if result.rowcount > 0:
                await self._update_metadata(autocommit, update_accessed_at=True, update_modified_at=True)

                await autocommit.commit()

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        """Iterate over the existing keys in the key-value store."""
        # Build query for record metadata
        stmt = (
            select(self._ITEM_TABLE.key, self._ITEM_TABLE.content_type, self._ITEM_TABLE.size)
            .where(self._ITEM_TABLE.metadata_id == self._id)
            .order_by(self._ITEM_TABLE.key)
        )

        # Apply exclusive_start_key filter
        if exclusive_start_key is not None:
            stmt = stmt.where(self._ITEM_TABLE.key > exclusive_start_key)

        # Apply limit
        if limit is not None:
            stmt = stmt.limit(limit)

        async with self.get_session() as session:
            result = await session.stream(stmt.execution_options(stream_results=True))

            async for row in result:
                yield KeyValueStoreRecordMetadata(
                    key=row.key,
                    content_type=row.content_type,
                    size=row.size,
                )

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

    @override
    async def record_exists(self, *, key: str) -> bool:
        """Check if a record with the given key exists in the key-value store."""
        stmt = select(self._ITEM_TABLE.key).where(self._ITEM_TABLE.metadata_id == self._id, self._ITEM_TABLE.key == key)
        async with self.get_session() as session:
            # Check if record exists
            result = await session.execute(stmt)

            updated = await self._update_metadata(session, update_accessed_at=True)

            # Commit updates to the metadata
            if updated:
                await session.commit()

            return result.scalar_one_or_none() is not None

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for SQL key-value stores.')

    def _specific_update_metadata(self, **_kwargs: dict[str, Any]) -> dict[str, Any]:
        return {}
