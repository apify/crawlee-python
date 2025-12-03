from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast, overload

from sqlalchemy import delete, select, text, update
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
        DatasetMetadataDb,
        KeyValueStoreMetadataDb,
        KeyValueStoreRecordDb,
        RequestDb,
        RequestQueueMetadataDb,
    )
    from ._storage_client import SqlStorageClient


logger = getLogger(__name__)


class MetadataUpdateParams(TypedDict, total=False):
    """Parameters for updating metadata."""

    update_accessed_at: NotRequired[bool]
    update_modified_at: NotRequired[bool]
    force: NotRequired[bool]


class SqlClientMixin(ABC):
    """Mixin class for SQL clients.

    This mixin provides common SQL operations and basic methods for SQL storage clients.
    """

    _DEFAULT_NAME: ClassVar[str]
    """Default name when none provided."""

    _METADATA_TABLE: ClassVar[type[DatasetMetadataDb | KeyValueStoreMetadataDb | RequestQueueMetadataDb]]
    """SQLAlchemy model for metadata."""

    _ITEM_TABLE: ClassVar[type[DatasetItemDb | KeyValueStoreRecordDb | RequestDb]]
    """SQLAlchemy model for items."""

    _CLIENT_TYPE: ClassVar[str]
    """Human-readable client type for error messages."""

    def __init__(self, *, id: str, storage_client: SqlStorageClient) -> None:
        self._id = id
        self._storage_client = storage_client

        # Time tracking to reduce database writes during frequent operation
        self._accessed_at_allow_update_after: datetime | None = None
        self._modified_at_allow_update_after: datetime | None = None
        self._accessed_modified_update_interval = storage_client.get_accessed_modified_update_interval()

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
            await client._update_metadata(session, update_accessed_at=True)
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
            client._accessed_at_allow_update_after = now + client._accessed_modified_update_interval
            client._modified_at_allow_update_after = now + client._accessed_modified_update_interval
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
        stmt = delete(self._ITEM_TABLE).where(self._ITEM_TABLE.storage_id == self._id)
        async with self.get_session(with_simple_commit=True) as session:
            await session.execute(stmt)
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
        async with self.get_session() as session:
            orm_metadata = await session.get(self._METADATA_TABLE, self._id)
            if not orm_metadata:
                raise ValueError(f'{self._CLIENT_TYPE} with ID "{self._id}" not found.')

            return metadata_model.model_validate(orm_metadata)

    def _default_update_metadata(
        self, *, update_accessed_at: bool = False, update_modified_at: bool = False, force: bool = False
    ) -> dict[str, Any]:
        """Prepare common metadata updates with rate limiting.

        Args:
            update_accessed_at: Whether to update accessed_at timestamp.
            update_modified_at: Whether to update modified_at timestamp.
            force: Whether to force the update regardless of rate limiting.
        """
        values_to_set: dict[str, Any] = {}
        now = datetime.now(timezone.utc)

        # If the record must be updated (for example, when updating counters), we update timestamps and shift the time.
        if force:
            if update_modified_at:
                values_to_set['modified_at'] = now
                self._modified_at_allow_update_after = now + self._accessed_modified_update_interval
            if update_accessed_at:
                values_to_set['accessed_at'] = now
                self._accessed_at_allow_update_after = now + self._accessed_modified_update_interval

        elif update_modified_at and (
            self._modified_at_allow_update_after is None or now >= self._modified_at_allow_update_after
        ):
            values_to_set['modified_at'] = now
            self._modified_at_allow_update_after = now + self._accessed_modified_update_interval
            # The record will be updated, we can update `accessed_at` and shift the time.
            if update_accessed_at:
                values_to_set['accessed_at'] = now
                self._accessed_at_allow_update_after = now + self._accessed_modified_update_interval

        elif update_accessed_at and (
            self._accessed_at_allow_update_after is None or now >= self._accessed_at_allow_update_after
        ):
            values_to_set['accessed_at'] = now
            self._accessed_at_allow_update_after = now + self._accessed_modified_update_interval

        return values_to_set

    @abstractmethod
    def _specific_update_metadata(self, **kwargs: Any) -> dict[str, Any]:
        """Prepare storage-specific metadata updates.

        Must be implemented by concrete classes.

        Args:
            **kwargs: Storage-specific update parameters.
        """

    async def _update_metadata(
        self,
        session: AsyncSession,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        force: bool = False,
        **kwargs: Any,
    ) -> bool:
        """Update storage metadata combining common and specific fields.

        Args:
            session: Active database session.
            update_accessed_at: Whether to update accessed_at timestamp.
            update_modified_at: Whether to update modified_at timestamp.
            force: Whether to force the update timestamps regardless of rate limiting.
            **kwargs: Additional arguments for _specific_update_metadata.

        Returns:
            True if any updates were made, False otherwise
        """
        values_to_set = self._default_update_metadata(
            update_accessed_at=update_accessed_at, update_modified_at=update_modified_at, force=force
        )

        values_to_set.update(self._specific_update_metadata(**kwargs))

        if values_to_set:
            if (stmt := values_to_set.pop('custom_stmt', None)) is None:
                stmt = update(self._METADATA_TABLE).where(self._METADATA_TABLE.id == self._id)

            stmt = stmt.values(**values_to_set)
            await session.execute(stmt)
            return True

        return False
