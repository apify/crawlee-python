from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_backends import SqlStorageBackend
from crawlee.storages import Dataset

if TYPE_CHECKING:
    from pathlib import Path


async def test_sqlite_wal_mode_with_default_connection(tmp_path: Path) -> None:
    """Test that WAL mode is applied for the default SQLite connection."""
    configuration = Configuration(storage_dir=str(tmp_path))

    async with SqlStorageBackend() as storage_backend:
        await storage_backend.initialize(configuration)

        async with storage_backend.engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() == 'wal'


async def test_sqlite_wal_mode_with_connection_string(tmp_path: Path) -> None:
    """Test that WAL mode is applied when using a custom SQLite connection string."""
    db_path = tmp_path / 'test.db'
    configuration = Configuration(storage_dir=str(tmp_path))

    async with SqlStorageBackend(connection_string=f'sqlite+aiosqlite:///{db_path}') as storage_backend:
        await storage_backend.initialize(configuration)

        async with storage_backend.engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() == 'wal'


async def test_sqlite_wal_mode_not_applied_with_custom_engine(tmp_path: Path) -> None:
    """Test that WAL mode is not applied when using a user-provided engine."""
    db_path = tmp_path / 'test.db'
    configuration = Configuration(storage_dir=str(tmp_path))
    engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}', future=True)

    async with SqlStorageBackend(engine=engine) as storage_backend:
        await storage_backend.initialize(configuration)

        async with engine.begin() as conn:
            result = await conn.execute(text('PRAGMA journal_mode'))
            assert result.scalar() != 'wal'

    await engine.dispose()


async def test_closed_backend_raises(tmp_path: Path) -> None:
    """A closed backend refuses to open storages and to serve the ones opened before."""
    configuration = Configuration(storage_dir=str(tmp_path))

    async with SqlStorageBackend() as storage_backend:
        kvs_backend = await storage_backend.create_kvs_backend(configuration=configuration)

    with pytest.raises(RuntimeError, match=r'SqlStorageBackend is closed'):
        await kvs_backend.set_value(key='key', value='value')

    with pytest.raises(RuntimeError, match=r'SqlStorageBackend is closed'):
        await storage_backend.create_kvs_backend(configuration=configuration)

    with pytest.raises(RuntimeError, match=r'SqlStorageBackend is closed'):
        _ = storage_backend.engine


async def test_backend_closed_before_first_use_refuses_initialization(tmp_path: Path) -> None:
    """A backend closed before its first use doesn't create an engine afterwards."""
    async with SqlStorageBackend() as storage_backend:
        pass

    with pytest.raises(RuntimeError, match=r'SqlStorageBackend is closed'):
        await storage_backend.initialize(Configuration(storage_dir=str(tmp_path)))


async def test_close_keeps_passed_engine(tmp_path: Path) -> None:
    """An engine passed in belongs to the caller, so the backend doesn't dispose it."""
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path / "test.db"}')

    with patch.object(AsyncEngine, 'dispose', AsyncMock()) as dispose:
        async with SqlStorageBackend(engine=engine):
            pass

    dispose.assert_not_awaited()
    await engine.dispose()


async def test_close_disposes_own_engine(tmp_path: Path) -> None:
    """An engine created from the connection string is disposed with the backend, and only once."""
    storage_backend = SqlStorageBackend(connection_string=f'sqlite+aiosqlite:///{tmp_path / "test.db"}')
    await storage_backend.initialize(Configuration(storage_dir=str(tmp_path)))
    engine = storage_backend.engine

    with patch.object(AsyncEngine, 'dispose', AsyncMock()) as dispose:
        await storage_backend.close()
        await storage_backend.close()

    dispose.assert_awaited_once()
    await engine.dispose()


async def test_new_backend_skips_storages_of_closed_one(tmp_path: Path) -> None:
    """A new backend opens its own storage instead of the cached one bound to a closed backend."""
    configuration = Configuration(storage_dir=str(tmp_path), purge_on_start=False)

    async with SqlStorageBackend() as storage_backend:
        old_dataset = await Dataset.open(name='results', storage_backend=storage_backend, configuration=configuration)

    async with SqlStorageBackend() as storage_backend:
        dataset = await Dataset.open(name='results', storage_backend=storage_backend, configuration=configuration)
        await dataset.push_data({'key': 'value'})

    assert dataset is not old_dataset


async def test_new_backend_purges_default_dataset(tmp_path: Path) -> None:
    """A new backend with `purge_on_start` enabled purges the default dataset an earlier backend wrote."""
    configuration = Configuration(storage_dir=str(tmp_path), purge_on_start=True)

    async with SqlStorageBackend() as storage_backend:
        dataset = await Dataset.open(storage_backend=storage_backend, configuration=configuration)
        await dataset.push_data({'key': 'value'})

    async with SqlStorageBackend() as storage_backend:
        dataset = await Dataset.open(storage_backend=storage_backend, configuration=configuration)
        data = await dataset.get_data()

    assert data.items == []


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None for mod_name in sys.modules if mod_name == 'sqlalchemy' or mod_name.startswith('sqlalchemy.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.storage_backends._sql'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.storage_backends._sql import SqlStorageBackend  # noqa: F401 PLC0415
