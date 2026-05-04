from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_clients import SqlStorageClient

if TYPE_CHECKING:
    from pathlib import Path


async def test_sqlite_wal_mode_with_default_connection(tmp_path: Path) -> None:
    """Test that WAL mode is applied for the default SQLite connection."""
    configuration = Configuration(storage_dir=str(tmp_path))

    async with SqlStorageClient() as storage_client:
        await storage_client.initialize(configuration)

        async with storage_client.engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() == 'wal'


async def test_sqlite_wal_mode_with_connection_string(tmp_path: Path) -> None:
    """Test that WAL mode is applied when using a custom SQLite connection string."""
    db_path = tmp_path / 'test.db'
    configuration = Configuration(storage_dir=str(tmp_path))

    async with SqlStorageClient(connection_string=f'sqlite+aiosqlite:///{db_path}') as storage_client:
        await storage_client.initialize(configuration)

        async with storage_client.engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() == 'wal'


async def test_sqlite_wal_mode_not_applied_with_custom_engine(tmp_path: Path) -> None:
    """Test that WAL mode is not applied when using a user-provided engine."""
    db_path = tmp_path / 'test.db'
    configuration = Configuration(storage_dir=str(tmp_path))
    engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}', future=True)

    async with SqlStorageClient(engine=engine) as storage_client:
        await storage_client.initialize(configuration)

        async with engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() != 'wal'
