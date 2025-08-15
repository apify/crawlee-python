from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from typing_extensions import override

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._db_models import KeyValueStoreMetadataDB, KeyValueStoreRecordDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient


logger = getLogger(__name__)


class SQLKeyValueStoreClient(KeyValueStoreClient):
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
    multiple clients access the same store using composite primary keys (kvs_id, key).
    """

    _DEFAULT_NAME_DB = 'default'
    """Default dataset name used when no name is provided."""

    def __init__(
        self,
        *,
        storage_client: SQLStorageClient,
        id: str,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SQLKeyValueStoreClient.open` class method to create a new instance.
        """
        self._id = id

        self._storage_client = storage_client
        """The storage client used to access the SQL database."""

        # Time tracking to reduce database writes during frequent operation
        self._last_accessed_at: datetime | None = None
        self._last_modified_at: datetime | None = None
        self._accessed_modified_update_interval = storage_client.get_accessed_modified_update_interval()

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        """Get the metadata for this key-value store."""
        # The database is a single place of truth
        async with self.get_session() as session:
            orm_metadata: KeyValueStoreMetadataDB | None = await session.get(KeyValueStoreMetadataDB, self._id)
            if not orm_metadata:
                raise ValueError(f'Key-value store with ID "{self._id}" not found.')

            return KeyValueStoreMetadata.model_validate(orm_metadata)

    def get_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this key-value store."""
        return self._storage_client.create_session()

    @asynccontextmanager
    async def get_autocommit_session(self) -> AsyncIterator[AsyncSession]:
        """Create a new SQLAlchemy autocommit session to insert, delete, or modify data."""
        async with self.get_session() as session:
            try:
                yield session
                await session.commit()
            except SQLAlchemyError as e:
                logger.warning(f'Error occurred during session transaction: {e}')
                # Rollback the session in case of an error
                await session.rollback()

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        storage_client: SQLStorageClient,
    ) -> SQLKeyValueStoreClient:
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
        async with storage_client.create_session() as session:
            orm_metadata: KeyValueStoreMetadataDB | None = None
            if id:
                orm_metadata = await session.get(KeyValueStoreMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Key-value store with ID "{id}" not found.')
            else:
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(KeyValueStoreMetadataDB).where(KeyValueStoreMetadataDB.name == search_name)
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
                metadata = KeyValueStoreMetadata(
                    id=crypto_random_object_id(),
                    name=name,
                    created_at=now,
                    accessed_at=now,
                    modified_at=now,
                )
                client = cls(
                    id=metadata.id,
                    storage_client=storage_client,
                )
                session.add(KeyValueStoreMetadataDB(**metadata.model_dump()))

            try:
                # Commit the insert or update metadata to the database
                await session.commit()
            except SQLAlchemyError:
                # Attempt to open simultaneously by different clients.
                # The commit that created the record has already been executed, make rollback and get by name.
                await session.rollback()
                search_name = name or cls._DEFAULT_NAME_DB
                stmt = select(KeyValueStoreMetadataDB).where(KeyValueStoreMetadataDB.name == search_name)
                result = await session.execute(stmt)
                orm_metadata = result.scalar_one_or_none()
                if not orm_metadata:
                    raise ValueError(f'Key-value store with Name "{search_name}" not found.') from None
                client = cls(
                    id=orm_metadata.id,
                    storage_client=storage_client,
                )
            return client

    @override
    async def drop(self) -> None:
        """Delete this key-value store and all its records from the database.

        This operation is irreversible. Uses CASCADE deletion to remove all related records.
        """
        stmt = delete(KeyValueStoreMetadataDB).where(KeyValueStoreMetadataDB.id == self._id)
        async with self.get_autocommit_session() as autosession:
            if self._storage_client.get_default_flag():
                # foreign_keys=ON is set at the connection level. Required for cascade deletion.
                await autosession.execute(text('PRAGMA foreign_keys=ON'))
            await autosession.execute(stmt)

    @override
    async def purge(self) -> None:
        """Remove all items from this key-value store while keeping the key-value store structure."""
        stmt = delete(KeyValueStoreRecordDB).filter_by(kvs_id=self._id)
        async with self.get_autocommit_session() as autosession:
            await autosession.execute(stmt)

            await self._update_metadata(autosession, update_accessed_at=True, update_modified_at=True)

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
        record_db = KeyValueStoreRecordDB(
            kvs_id=self._id,
            key=key,
            value=value_bytes,
            content_type=content_type,
            size=size,
        )

        stmt = (
            update(KeyValueStoreRecordDB)
            .where(KeyValueStoreRecordDB.kvs_id == self._id, KeyValueStoreRecordDB.key == key)
            .values(value=value_bytes, content_type=content_type, size=size)
        )

        # A race condition is possible if several clients work with one kvs.
        # Unfortunately, there is no implementation of atomic Upsert that is independent of specific dialects.
        # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-upsert-statements
        async with self.get_session() as session:
            result = await session.execute(stmt)
            if result.rowcount == 0:
                session.add(record_db)

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
        stmt = select(KeyValueStoreRecordDB).where(
            KeyValueStoreRecordDB.kvs_id == self._id, KeyValueStoreRecordDB.key == key
        )
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
        stmt = delete(KeyValueStoreRecordDB).where(
            KeyValueStoreRecordDB.kvs_id == self._id, KeyValueStoreRecordDB.key == key
        )
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
            select(KeyValueStoreRecordDB.key, KeyValueStoreRecordDB.content_type, KeyValueStoreRecordDB.size)
            .where(KeyValueStoreRecordDB.kvs_id == self._id)
            .order_by(KeyValueStoreRecordDB.key)
        )

        # Apply exclusive_start_key filter
        if exclusive_start_key is not None:
            stmt = stmt.where(KeyValueStoreRecordDB.key > exclusive_start_key)

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
        stmt = select(KeyValueStoreRecordDB.key).where(
            KeyValueStoreRecordDB.kvs_id == self._id, KeyValueStoreRecordDB.key == key
        )
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
        raise NotImplementedError('Public URLs are not supported for memory key-value stores.')

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
    ) -> bool:
        """Update the KVS metadata in the database.

        Args:
            session: The SQLAlchemy AsyncSession to use for the update.
            update_accessed_at: If True, update the `accessed_at` timestamp to the current time.
            update_modified_at: If True, update the `modified_at` timestamp to the current time.
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

        if values_to_set:
            stmt = update(KeyValueStoreMetadataDB).where(KeyValueStoreMetadataDB.id == self._id).values(**values_to_set)
            await session.execute(stmt)
            return True

        return False
