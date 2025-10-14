from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import CursorResult, delete, select
from typing_extensions import Self, override

from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._client_mixin import MetadataUpdateParams, SqlClientMixin
from ._db_models import KeyValueStoreMetadataDb, KeyValueStoreRecordDb

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

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

    _METADATA_TABLE = KeyValueStoreMetadataDb
    """SQLAlchemy model for key-value store metadata."""

    _ITEM_TABLE = KeyValueStoreRecordDb
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

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        # The database is a single place of truth
        return await self._get_metadata(KeyValueStoreMetadata)

    @override
    async def drop(self) -> None:
        """Delete this key-value store and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        await self._drop()

    @override
    async def purge(self) -> None:
        """Remove all items from this key-value store while keeping the key-value store structure.

        Remove all records from key_value_store_records table.
        """
        await self._purge(metadata_kwargs=MetadataUpdateParams(update_accessed_at=True, update_modified_at=True))

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

            await self._update_metadata(
                session, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True)
            )

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # Query the record by key
        stmt = select(self._ITEM_TABLE).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session() as session:
            result = await session.execute(stmt)
            record_db = result.scalar_one_or_none()

            updated = await self._update_metadata(session, **MetadataUpdateParams(update_accessed_at=True))

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
        stmt = delete(self._ITEM_TABLE).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session(with_simple_commit=True) as session:
            # Delete the record if it exists
            result = await session.execute(stmt)
            result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result

            # Update metadata if we actually deleted something
            if result.rowcount > 0:
                await self._update_metadata(
                    session, **MetadataUpdateParams(update_accessed_at=True, update_modified_at=True)
                )

                await session.commit()

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

        async with self.get_session() as session:
            result = await session.stream(stmt.execution_options(stream_results=True))

            async for row in result:
                yield KeyValueStoreRecordMetadata(
                    key=row.key,
                    content_type=row.content_type,
                    size=row.size,
                )

            updated = await self._update_metadata(session, **MetadataUpdateParams(update_accessed_at=True))

            # Commit updates to the metadata
            if updated:
                await session.commit()

    @override
    async def record_exists(self, *, key: str) -> bool:
        stmt = select(self._ITEM_TABLE.key).where(
            self._ITEM_TABLE.key_value_store_id == self._id, self._ITEM_TABLE.key == key
        )
        async with self.get_session() as session:
            # Check if record exists
            result = await session.execute(stmt)

            updated = await self._update_metadata(session, **MetadataUpdateParams(update_accessed_at=True))

            # Commit updates to the metadata
            if updated:
                await session.commit()

            return result.scalar_one_or_none() is not None

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for SQL key-value stores.')

    def _specific_update_metadata(self, **_kwargs: dict[str, Any]) -> dict[str, Any]:
        return {}
