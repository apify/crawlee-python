from __future__ import annotations

from contextlib import asynccontextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as lite_insert
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Insert
    from sqlalchemy.ext.asyncio import AsyncSession

    from ._storage_client import SQLStorageClient


logger = getLogger(__name__)


class SQLClientMixin:
    """Mixin class for SQL clients."""

    _storage_client: SQLStorageClient

    def get_session(self) -> AsyncSession:
        """Create a new SQLAlchemy session for this request queue."""
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

    def build_insert_stmt_with_ignore(self, table_model: Any, insert_values: dict | list[dict]) -> Insert:
        """Build an insert statement with ignore for the SQL dialect."""
        if isinstance(insert_values, dict):
            insert_values = [insert_values]

        dialect = self._storage_client.get_dialect_name()

        if dialect == 'postgresql':
            return pg_insert(table_model).values(insert_values).on_conflict_do_nothing()

        if dialect == 'mysql':
            return mysql_insert(table_model).values(insert_values).on_duplicate_key_update()

        if dialect == 'sqlite':
            return lite_insert(table_model).values(insert_values).on_conflict_do_nothing()

        raise NotImplementedError(f'Insert with ignore not supported for dialect: {dialect}')

    def build_upsert_stmt(
        self,
        table_model: Any,
        insert_values: dict | list[dict],
        update_columns: list[str],
        conflict_cols: list[str] | None = None,
    ) -> Insert:
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

        if dialect == 'mysql':
            mysql_stmt = mysql_insert(table_model).values(insert_values)
            set_ = {col: mysql_stmt.inserted[col] for col in update_columns}
            return mysql_stmt.on_duplicate_key_update(**set_)

        raise NotImplementedError(f'Upsert not supported for dialect: {dialect}')
