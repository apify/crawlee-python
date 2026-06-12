from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
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


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None for mod_name in sys.modules if mod_name == 'sqlalchemy' or mod_name.startswith('sqlalchemy.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.storage_clients._sql'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.storage_clients._sql import SqlStorageClient  # noqa: F401 PLC0415
