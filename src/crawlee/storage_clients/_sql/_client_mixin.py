from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast, overload

from sqlalchemy import CursorResult, delete, select, text, update
from sqlalchemy import func as sql_func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as lite_insert
from sqlalchemy.exc import SQLAlchemyError

from crawlee._utils.crypto import crypto_random_object_id

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Insert
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import DeclarativeBase
    from typing_extensions import NotRequired, Self

    from crawlee.storage_clients.models import DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata

    from ._db_models import (
        DatasetItemDb,
        DatasetMetadataBufferDb,
        DatasetMetadataDb,
        KeyValueStoreMetadataBufferDb,
        KeyValueStoreMetadataDb,
        KeyValueStoreRecordDb,
        RequestDb,
        RequestQueueMetadataBufferDb,
        RequestQueueMetadataDb,
    )
    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class MetadataUpdateParams(TypedDict, total=False):
    """Parameters for updating metadata."""

    accessed_at: NotRequired[datetime]
    modified_at: NotRequired[datetime]


class SqlClientMixin(ABC):
    """Mixin class for SQL clients.

    This mixin provides common SQL operations and basic methods for SQL storage clients.
    """

    _DEFAULT_NAME: ClassVar[str]
    """Default name when none provided."""

    _METADATA_TABLE: ClassVar[type[DatasetMetadataDb | KeyValueStoreMetadataDb | RequestQueueMetadataDb]]
    """SQLAlchemy model for metadata."""

    _BUFFER_TABLE: ClassVar[
        type[KeyValueStoreMetadataBufferDb | DatasetMetadataBufferDb | RequestQueueMetadataBufferDb]
    ]
    """SQLAlchemy model for metadata buffer."""

    _ITEM_TABLE: ClassVar[type[DatasetItemDb | KeyValueStoreRecordDb | RequestDb]]
    """SQLAlchemy model for items."""

    _CLIENT_TYPE: ClassVar[str]
    """Human-readable client type for error messages."""

    _BLOCK_BUFFER_TIME = timedelta(seconds=1)
    """Time interval that blocks buffer reading to update metadata."""

    def __init__(self, *, id: str, storage_client: SqlStorageClient) -> None:
        self._id = id
        self._storage_client = storage_client

    @classmethod
    async def _open(
        cls,
        *,
        id: str | None,
        name: str | None,
        internal_name: str,
        storage_client: SqlStorageClient,
        metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata],
        session: AsyncSession,
        extra_metadata_fields: dict[str, Any],
    ) -> Self:
        """Open existing storage or create new one.

        Internal method used by _safely_open.

        Args:
            id: Storage ID to open (takes precedence over name).
            name: The name of the storage.
            internal_name: The database name for the storage based on name or alias.
            storage_client: SQL storage client instance.
            metadata_model: Pydantic model for metadata validation.
            session: Active database session.
            extra_metadata_fields: Storage-specific metadata fields.
        """
        orm_metadata: DatasetMetadataDb | KeyValueStoreMetadataDb | RequestQueueMetadataDb | None = None
        if id:
            orm_metadata = await session.get(cls._METADATA_TABLE, id)
            if not orm_metadata:
                raise ValueError(f'{cls._CLIENT_TYPE} with ID "{id}" not found.')
        else:
            stmt = select(cls._METADATA_TABLE).where(cls._METADATA_TABLE.internal_name == internal_name)
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()  # type: ignore[assignment]

        if orm_metadata:
            client = cls(id=orm_metadata.id, storage_client=storage_client)
            await client._add_buffer_record(session)
            # Ensure any pending buffer updates are processed
            await client._process_buffers()
        else:
            now = datetime.now(timezone.utc)
            metadata = metadata_model(
                id=crypto_random_object_id(),
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                **extra_metadata_fields,
            )
            client = cls(id=metadata.id, storage_client=storage_client)
            session.add(cls._METADATA_TABLE(**metadata.model_dump(), internal_name=internal_name))

        return client

    @classmethod
    async def _safely_open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None = None,
        storage_client: SqlStorageClient,
        metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata],
        extra_metadata_fields: dict[str, Any],
    ) -> Self:
        """Safely open storage with transaction handling.

        Args:
            id: Storage ID to open (takes precedence over name).
            name: The name of the storage for named (global scope) storages.
            alias: The alias of the storage for unnamed (run scope) storages.
            storage_client: SQL storage client instance.
            client_class: Concrete client class to instantiate.
            metadata_model: Pydantic model for metadata validation.
            extra_metadata_fields: Storage-specific metadata fields.
        """
        # Validate input parameters.
        specified_params = sum(1 for param in [id, name, alias] if param is not None)
        if specified_params > 1:
            raise ValueError('Only one of "id", "name", or "alias" can be specified, not multiple.')

        internal_name = name or alias or cls._DEFAULT_NAME

        async with storage_client.create_session() as session:
            try:
                client = await cls._open(
                    id=id,
                    name=name,
                    internal_name=internal_name,
                    storage_client=storage_client,
                    metadata_model=metadata_model,
                    session=session,
                    extra_metadata_fields=extra_metadata_fields,
                )
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()

                stmt = select(cls._METADATA_TABLE).where(cls._METADATA_TABLE.internal_name == internal_name)
                result = await session.execute(stmt)
                orm_metadata: DatasetMetadataDb | KeyValueStoreMetadataDb | RequestQueueMetadataDb | None
                orm_metadata = cast(
                    'DatasetMetadataDb | KeyValueStoreMetadataDb | RequestQueueMetadataDb | None',
                    result.scalar_one_or_none(),
                )

                if not orm_metadata:
                    raise ValueError(f'{cls._CLIENT_TYPE} with Name "{internal_name}" not found.') from None

                client = cls(id=orm_metadata.id, storage_client=storage_client)

        return client

    @asynccontextmanager
    async def get_session(self, *, with_simple_commit: bool = False) -> AsyncIterator[AsyncSession]:
        """Create a new SQLAlchemy session for this storage."""
        async with self._storage_client.create_session() as session:
            # For operations where a final commit is mandatory and does not require specific processing conditions
            if with_simple_commit:
                try:
                    yield session
                    await session.commit()
                except SQLAlchemyError as e:
                    logger.warning(f'Error occurred during session transaction: {e}')
                    await session.rollback()
            else:
                yield session

    def _build_insert_stmt_with_ignore(
        self, table_model: type[DeclarativeBase], insert_values: dict[str, Any] | list[dict[str, Any]]
    ) -> Insert:
        """Build an insert statement with ignore for the SQL dialect.

        Args:
            table_model: SQLAlchemy table model.
            insert_values: Single dict or list of dicts to insert.
        """
        if isinstance(insert_values, dict):
            insert_values = [insert_values]

        dialect = self._storage_client.get_dialect_name()

        if dialect == 'postgresql':
            return pg_insert(table_model).values(insert_values).on_conflict_do_nothing()

        if dialect == 'sqlite':
            return lite_insert(table_model).values(insert_values).on_conflict_do_nothing()

        raise NotImplementedError(f'Insert with ignore not supported for dialect: {dialect}')

    def _build_upsert_stmt(
        self,
        table_model: type[DeclarativeBase],
        insert_values: dict[str, Any] | list[dict[str, Any]],
        update_columns: list[str],
        conflict_cols: list[str] | None = None,
    ) -> Insert:
        """Build an upsert statement for the SQL dialect.

        Args:
            table_model: SQLAlchemy table model.
            insert_values: Single dict or list of dicts to upsert.
            update_columns: Column names to update on conflict.
            conflict_cols: Column names that define uniqueness (for PostgreSQL/SQLite).

        """
        if isinstance(insert_values, dict):
            insert_values = [insert_values]

        dialect = self._storage_client.get_dialect_name()

        if dialect == 'postgresql':
            pg_stmt = pg_insert(table_model).values(insert_values)
            set_ = {col: getattr(pg_stmt.excluded, col) for col in update_columns}
            return pg_stmt.on_conflict_do_update(index_elements=conflict_cols, set_=set_)

        if dialect == 'sqlite':
            lite_stmt = lite_insert(table_model).values(insert_values)
            set_ = {col: getattr(lite_stmt.excluded, col) for col in update_columns}
            return lite_stmt.on_conflict_do_update(index_elements=conflict_cols, set_=set_)

        raise NotImplementedError(f'Upsert not supported for dialect: {dialect}')

    async def _purge(self, metadata_kwargs: MetadataUpdateParams) -> None:
        """Drop all items in storage and update metadata.

        Args:
            metadata_kwargs: Arguments to pass to _update_metadata.
        """
        # Process buffers to ensure metadata is up to date before purging
        await self._process_buffers()

        stmt_records = delete(self._ITEM_TABLE).where(self._ITEM_TABLE.storage_id == self._id)
        stmt_buffers = delete(self._BUFFER_TABLE).where(self._BUFFER_TABLE.storage_id == self._id)
        async with self.get_session(with_simple_commit=True) as session:
            await session.execute(stmt_records)
            await session.execute(stmt_buffers)
            await self._update_metadata(session, **metadata_kwargs)

    async def _drop(self) -> None:
        """Delete this storage and all its data.

        This operation is irreversible. Uses CASCADE deletion to remove all related items.
        """
        stmt = delete(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id)
        async with self.get_session(with_simple_commit=True) as session:
            if self._storage_client.get_dialect_name() == 'sqlite':
                # foreign_keys=ON is set at the connection level. Required for cascade deletion.
                await session.execute(text('PRAGMA foreign_keys=ON'))
            await session.execute(stmt)

    @overload
    async def _get_metadata(self, metadata_model: type[DatasetMetadata]) -> DatasetMetadata: ...
    @overload
    async def _get_metadata(self, metadata_model: type[KeyValueStoreMetadata]) -> KeyValueStoreMetadata: ...
    @overload
    async def _get_metadata(self, metadata_model: type[RequestQueueMetadata]) -> RequestQueueMetadata: ...

    async def _get_metadata(
        self, metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata]
    ) -> DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata:
        """Retrieve client metadata."""
        # Process any pending buffer updates first
        await self._process_buffers()

        async with self.get_session() as session:
            orm_metadata = await session.get(self._METADATA_TABLE, self._id)
            if not orm_metadata:
                raise ValueError(f'{self._CLIENT_TYPE} with ID "{self._id}" not found.')

            return metadata_model.model_validate(orm_metadata)

    @abstractmethod
    def _specific_update_metadata(self, **kwargs: Any) -> dict[str, Any]:
        """Prepare storage-specific metadata updates.

        Must be implemented by concrete classes.

        Args:
            **kwargs: Storage-specific update parameters.
        """

    @abstractmethod
    def _prepare_buffer_data(self, **kwargs: Any) -> dict[str, Any]:
        """Prepare storage-specific buffer data. Must be implemented by concrete classes."""

    @abstractmethod
    async def _apply_buffer_updates(self, session: AsyncSession, max_buffer_id: int) -> None:
        """Apply aggregated buffer updates to metadata. Must be implemented by concrete classes.

        Args:
            session: Active database session.
            max_buffer_id: Maximum buffer record ID to process.
        """

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        accessed_at: datetime | None = None,
        modified_at: datetime | None = None,
        **kwargs: Any,
    ) -> None:
        """Directly update storage metadata combining common and specific fields.

        Args:
            session: Active database session.
            accessed_at: Datetime to set as accessed_at timestamp.
            modified_at: Datetime to set as modified_at timestamp.
            **kwargs: Additional arguments for _specific_update_metadata.
        """
        values_to_set: dict[str, Any] = {}

        if accessed_at is not None:
            values_to_set['accessed_at'] = accessed_at

        if modified_at is not None:
            values_to_set['modified_at'] = modified_at

        values_to_set.update(self._specific_update_metadata(**kwargs))

        if values_to_set:
            if (stmt := values_to_set.pop('custom_stmt', None)) is None:
                stmt = update(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id)

            stmt = stmt.values(**values_to_set)
            await session.execute(stmt)

    async def _add_buffer_record(
        self,
        session: AsyncSession,
        *,
        update_modified_at: bool = False,
        **kwargs: Any,
    ) -> None:
        """Add a record to the buffer table and update metadata.

        Args:
            session: Active database session.
            update_modified_at: Whether to update modified_at timestamp.
            **kwargs: Additional arguments for _prepare_buffer_data.
        """
        now = datetime.now(timezone.utc)
        values_to_set = {
            'storage_id': self._id,
            'accessed_at': now,  # All entries in the buffer require updating `accessed_at`
            'modified_at': now if update_modified_at else None,
        }
        values_to_set.update(self._prepare_buffer_data(**kwargs))

        session.add(self._BUFFER_TABLE(**values_to_set))

    async def _try_acquire_buffer_lock(self, session: AsyncSession) -> bool:
        """Try to acquire buffer processing lock for 200ms.

        Args:
            session: Active database session.

        Returns:
            True if lock was acquired, False if already locked by another process.
        """
        now = datetime.now(timezone.utc)
        lock_until = now + self._BLOCK_BUFFER_TIME
        dialect = self._storage_client.get_dialect_name()

        if dialect == 'postgresql':
            select_stmt = (
                select(self._METADATA_TABLE)
                .where(
                    self._METADATA_TABLE.id == self._id,
                    (self._METADATA_TABLE.buffer_locked_until.is_(None))
                    | (self._METADATA_TABLE.buffer_locked_until < now),
                    select(self._BUFFER_TABLE.id).where(self._BUFFER_TABLE.storage_id == self._id).exists(),
                )
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(select_stmt)
            metadata_row = result.scalar_one_or_none()

            if metadata_row is None:
                # Either conditions not met OR row is locked by another process
                return False

        # Acquire lock only if not currently locked or lock has expired
        update_stmt = (
            update(self._METADATA_TABLE)
            .where(
                self._METADATA_TABLE.id == self._id,
                (self._METADATA_TABLE.buffer_locked_until.is_(None)) | (self._METADATA_TABLE.buffer_locked_until < now),
                select(self._BUFFER_TABLE.id).where(self._BUFFER_TABLE.storage_id == self._id).exists(),
            )
            .values(buffer_locked_until=lock_until)
        )

        result = await session.execute(update_stmt)
        result = cast('CursorResult', result) if not isinstance(result, CursorResult) else result

        if result.rowcount > 0:
            await session.flush()
            return True

        return False

    async def _release_buffer_lock(self, session: AsyncSession) -> None:
        """Release buffer processing lock by setting buffer_locked_until to NULL.

        Args:
            session: Active database session.
        """
        stmt = update(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id).values(buffer_locked_until=None)

        await session.execute(stmt)

        await session.flush()

    async def _has_pending_buffer_updates(self, session: AsyncSession) -> bool:
        """Check if there are pending buffer updates not yet applied to metadata.

        Returns False only when buffer_locked_until is NULL (metadata is consistent).

        Returns:
            True if metadata might be inconsistent due to pending buffer updates.
        """
        result = await session.execute(
            select(self._METADATA_TABLE.buffer_locked_until).where(self._METADATA_TABLE.id == self._id)
        )

        locked_until = result.scalar()

        # Any non-NULL value means there are pending updates
        return locked_until is not None

    async def _process_buffers(self) -> None:
        """Process pending buffer updates and apply them to metadata."""
        async with self.get_session(with_simple_commit=True) as session:
            # Try to acquire buffer processing lock
            if not await self._try_acquire_buffer_lock(session):
                # Another process is currently processing buffers or lock acquisition failed
                return

            # Get the maximum buffer ID at this moment
            # This creates a consistent snapshot - records added during processing won't be included
            max_buffer_id_stmt = select(sql_func.max(self._BUFFER_TABLE.id)).where(
                self._BUFFER_TABLE.storage_id == self._id
            )

            result = await session.execute(max_buffer_id_stmt)
            max_buffer_id = result.scalar()

            if max_buffer_id is None:
                # No buffer records to process. Release the lock and exit.
                await self._release_buffer_lock(session)
                return

            # Apply aggregated buffer updates to metadata using only records <= max_buffer_id
            # This method is implemented by concrete storage classes
            await self._apply_buffer_updates(session, max_buffer_id=max_buffer_id)

            # Clean up only the processed buffer records (those <= max_buffer_id)
            delete_stmt = delete(self._BUFFER_TABLE).where(
                self._BUFFER_TABLE.storage_id == self._id, self._BUFFER_TABLE.id <= max_buffer_id
            )

            await session.execute(delete_stmt)

            # Release the lock after successful processing
            await self._release_buffer_lock(session)
