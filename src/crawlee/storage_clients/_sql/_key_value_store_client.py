from __future__ import annotations

import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select
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

    This client persists data to a SQL database, making it suitable for scenarios where data needs to
    survive process restarts. Keys are mapped to rows in a database table.

    Binary data is stored as-is, while JSON and text data are stored in human-readable format.
    The implementation automatically handles serialization based on the content type and
    maintains metadata about each record.

    This implementation is ideal for long-running crawlers where persistence is important and
    for development environments where you want to easily inspect the stored data between runs.

    Binary data is stored as-is, while JSON and text data are stored in human-readable format.
    The implementation automatically handles serialization based on the content type and
    maintains metadata about each record.

    This implementation is ideal for long-running crawlers where persistence is important and
    for development environments where you want to easily inspect the stored data between runs.
    """

    def __init__(
        self,
        *,
        storage_client: SQLStorageClient,
        orm_metadata: KeyValueStoreMetadataDB,
    ) -> None:
        """Initialize a new instance.

        Preferably use the `SQLKeyValueStoreClient.open` class method to create a new instance.
        """
        self._orm_metadata = orm_metadata

        self._storage_client = storage_client
        """The storage client used to access the SQL database."""

    def create_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this key-value store."""
        return self._storage_client.create_session()

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return KeyValueStoreMetadata.model_validate(self._orm_metadata)

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
            if id:
                orm_metadata = await session.get(KeyValueStoreMetadataDB, id)
                if not orm_metadata:
                    raise ValueError(f'Key-value store with ID "{id}" not found.')
                client = cls(
                    orm_metadata=orm_metadata,
                    storage_client=storage_client,
                )
                client._update_metadata(update_accessed_at=True)

            else:
                orm_metadata = await session.get(KeyValueStoreMetadataDB, name)
                if orm_metadata:
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )
                    client._update_metadata(update_accessed_at=True)
                else:
                    now = datetime.now(timezone.utc)
                    metadata = KeyValueStoreMetadata(
                        id=crypto_random_object_id(),
                        name=name,
                        created_at=now,
                        accessed_at=now,
                        modified_at=now,
                    )
                    orm_metadata = KeyValueStoreMetadataDB(**metadata.model_dump())
                    client = cls(
                        orm_metadata=orm_metadata,
                        storage_client=storage_client,
                    )
                    session.add(orm_metadata)

            await session.commit()

            return client

    @override
    async def drop(self) -> None:
        async with self._storage_client.create_session() as session:
            kvs_db = await session.get(KeyValueStoreMetadataDB, self._orm_metadata.id)
            if kvs_db:
                await session.delete(kvs_db)
                await session.commit()

    @override
    async def purge(self) -> None:
        async with self._storage_client.create_session() as session:
            stmt = delete(KeyValueStoreRecordDB).filter_by(kvs_id=self._orm_metadata.id)
            await session.execute(stmt)

            self._update_metadata(update_accessed_at=True, update_modified_at=True)
            await session.commit()

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
        record_db = KeyValueStoreRecordDB(
            kvs_id=self._orm_metadata.id,
            key=key,
            value=value_bytes,
            content_type=content_type,
            size=size,
        )

        async with self._storage_client.create_session() as session:
            existing_record = await session.get(KeyValueStoreRecordDB, (self._orm_metadata.id, key))
            if existing_record:
                # Update existing record
                existing_record.value = value_bytes
                existing_record.content_type = content_type
                existing_record.size = size
            else:
                session.add(record_db)
            self._update_metadata(update_accessed_at=True, update_modified_at=True)
            await session.merge(self._orm_metadata)
            await session.commit()

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        # Update the metadata to record access
        async with self._storage_client.create_session() as session:
            stmt = select(KeyValueStoreRecordDB).where(
                KeyValueStoreRecordDB.kvs_id == self._orm_metadata.id, KeyValueStoreRecordDB.key == key
            )
            result = await session.execute(stmt)
            record_db = result.scalar_one_or_none()

            self._update_metadata(update_accessed_at=True)

            await session.merge(self._orm_metadata)
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
        async with self._storage_client.create_session() as session:
            # Delete the record if it exists
            stmt = delete(KeyValueStoreRecordDB).where(
                KeyValueStoreRecordDB.kvs_id == self._orm_metadata.id, KeyValueStoreRecordDB.key == key
            )
            result = await session.execute(stmt)

            # Update metadata if we actually deleted something
            if result.rowcount > 0:
                self._update_metadata(update_accessed_at=True, update_modified_at=True)
                await session.merge(self._orm_metadata)

            await session.commit()

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        async with self._storage_client.create_session() as session:
            # Build query for record metadata
            stmt = (
                select(KeyValueStoreRecordDB.key, KeyValueStoreRecordDB.content_type, KeyValueStoreRecordDB.size)
                .where(KeyValueStoreRecordDB.kvs_id == self._orm_metadata.id)
                .order_by(KeyValueStoreRecordDB.key)
            )

            # Apply exclusive_start_key filter
            if exclusive_start_key is not None:
                stmt = stmt.where(KeyValueStoreRecordDB.key > exclusive_start_key)

            # Apply limit
            if limit is not None:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)

            self._update_metadata(update_accessed_at=True)
            await session.merge(self._orm_metadata)
            await session.commit()

            for row in result:
                yield KeyValueStoreRecordMetadata(
                    key=row.key,
                    content_type=row.content_type,
                    size=row.size,
                )

    @override
    async def record_exists(self, *, key: str) -> bool:
        async with self._storage_client.create_session() as session:
            # Check if record exists
            stmt = select(KeyValueStoreRecordDB.key).where(
                KeyValueStoreRecordDB.kvs_id == self._orm_metadata.id, KeyValueStoreRecordDB.key == key
            )
            result = await session.execute(stmt)

            self._update_metadata(update_accessed_at=True)
            await session.merge(self._orm_metadata)
            await session.commit()

            return result.scalar_one_or_none() is not None

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for memory key-value stores.')

    def _update_metadata(
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
