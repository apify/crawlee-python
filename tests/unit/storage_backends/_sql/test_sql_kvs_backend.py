from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_backends import SqlStorageBackend
from crawlee.storage_backends._sql._db_models import KeyValueStoreMetadataDb, KeyValueStoreRecordDb
from crawlee.storage_backends.models import KeyValueStoreMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy import Connection

    from crawlee.storage_backends._sql import SqlKeyValueStoreBackend


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def kvs_backend(
    configuration: Configuration,
) -> AsyncGenerator[SqlKeyValueStoreBackend, None]:
    """A fixture for a SQL key-value store backend."""
    async with SqlStorageBackend() as storage_backend:
        backend = await storage_backend.create_kvs_backend(
            name='test-kvs',
            configuration=configuration,
        )
        yield backend
        await backend.drop()


# Helper function that allows you to use inspect with an asynchronous engine
def get_tables(sync_conn: Connection) -> list[str]:
    inspector = inspect(sync_conn)
    return inspector.get_table_names()


async def test_create_tables_with_connection_string(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL key-value store backend creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'

    async with SqlStorageBackend(connection_string=f'sqlite+aiosqlite:///{storage_dir}') as storage_backend:
        await storage_backend.create_kvs_backend(
            name='new-kvs',
            configuration=configuration,
        )

        async with storage_backend.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables


async def test_create_tables_with_engine(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL key-value store backend creates tables with a pre-configured engine."""
    storage_dir = tmp_path / 'test_table.db'

    engine = create_async_engine(f'sqlite+aiosqlite:///{storage_dir}', future=True, echo=False)

    async with SqlStorageBackend(engine=engine) as storage_backend:
        await storage_backend.create_kvs_backend(
            name='new-kvs',
            configuration=configuration,
        )

        async with engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables

    await engine.dispose()


async def test_tables_and_metadata_record(configuration: Configuration) -> None:
    """Test that SQL key-value store creates proper tables and metadata records."""
    async with SqlStorageBackend() as storage_backend:
        backend = await storage_backend.create_kvs_backend(
            name='new-kvs',
            configuration=configuration,
        )

        backend_metadata = await backend.get_metadata()

        async with storage_backend.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables

        async with backend.get_session() as session:
            stmt = select(KeyValueStoreMetadataDb).where(KeyValueStoreMetadataDb.name == 'new-kvs')
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()
            metadata = KeyValueStoreMetadata.model_validate(orm_metadata)
            assert metadata.id == backend_metadata.id
            assert metadata.name == 'new-kvs'

        await backend.drop()


async def test_value_record_creation(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that SQL key-value store backend can create a record."""
    test_key = 'test-key'
    test_value = b'Hello, world!'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain; charset=utf-8')
    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.content_type == 'text/plain; charset=utf-8'
        assert record.size == len(test_value)
        assert record.value == test_value


async def test_binary_data_persistence(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='application/octet-stream')

    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.content_type == 'application/octet-stream'
        assert record.size == len(test_value)
        assert record.value == test_value

    verify_record = await kvs_backend.get_value(key=test_key)
    assert verify_record is not None
    assert verify_record.value == test_value
    assert verify_record.content_type == 'application/octet-stream'


async def test_record_deletion_on_value_delete(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that deleting a value removes its record from the database."""
    test_key = 'test-delete'
    test_value = b'Delete me'

    # Set a value
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain')

    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.value == test_value

    # Delete the value
    await kvs_backend.delete_value(key=test_key)

    # Verify record was deleted
    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is None


async def test_drop_removes_records(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that drop removes all records from the database."""
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    backend_metadata = await kvs_backend.get_metadata()

    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == 'test')
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None

    # Drop the store
    await kvs_backend.drop()

    async with kvs_backend.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == 'test')
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is None
        metadata = await session.get(KeyValueStoreMetadataDb, backend_metadata.id)
        assert metadata is None


async def test_metadata_record_updates(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that read/write operations properly update metadata record timestamps."""
    # Record initial timestamps
    metadata = await kvs_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_backend.get_value(key='nonexistent')

    # Verify accessed timestamp was updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify modified timestamp was updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    async with kvs_backend.get_session() as session:
        orm_metadata = await session.get(KeyValueStoreMetadataDb, metadata.id)
        assert orm_metadata is not None
        assert orm_metadata.created_at == metadata.created_at
        assert orm_metadata.accessed_at == metadata.accessed_at
        assert orm_metadata.modified_at == metadata.modified_at


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same key-value store."""
    async with SqlStorageBackend() as storage_backend:
        original_backend = await storage_backend.create_kvs_backend(
            name='persistence-test',
            configuration=configuration,
        )

        test_key = 'persistent-key'
        test_value = b'persistent-value'
        await original_backend.set_value(key=test_key, value=test_value, content_type='text/plain')

        kvs_id = (await original_backend.get_metadata()).id

        # Reopen by ID and verify data persists
        reopened_backend = await storage_backend.create_kvs_backend(
            id=kvs_id,
            configuration=configuration,
        )

        record = await reopened_backend.get_value(key=test_key)
        assert record is not None
        assert record.value == test_value

        await reopened_backend.drop()


async def test_error_handling_on_set_failure(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that set_value properly handles SQL errors and retries."""
    with patch(
        'crawlee.storage_backends._sql._key_value_store_backend.SqlKeyValueStoreBackend.get_session',
    ) as mock_get_session:
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError('db error'))
        mock_get_session.return_value = mock_session

        with (
            patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
            pytest.raises(SQLAlchemyError),
        ):
            await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_set_value_does_not_retry_on_unexpected_exception(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """Test that set_value does not retry on unexpected exceptions."""
    with patch(
        'crawlee.storage_backends._sql._key_value_store_backend.SqlKeyValueStoreBackend.get_session',
    ) as mock_get_session:
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.execute = AsyncMock(side_effect=ValueError('unexpected error'))
        mock_get_session.return_value = mock_session

        with (
            patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
            pytest.raises(ValueError, match='unexpected error'),
        ):
            await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0


async def test_get_values_reads_records_with_single_query(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """`get_values` reads the records with one query, without calling `get_value` per key."""
    await kvs_backend.set_value(key='a-json', value=b'{"nested": [1, 2]}', content_type='application/json')
    await kvs_backend.set_value(key='b-text', value=b'plain text', content_type='text/plain')
    await kvs_backend.set_value(key='c-bytes', value=b'\x00\x01binary', content_type='application/octet-stream')

    with (
        patch.object(kvs_backend, 'get_value', side_effect=AssertionError('get_value must not be called')),
        patch.object(kvs_backend, 'get_session', wraps=kvs_backend.get_session) as get_session,
    ):
        records = await kvs_backend.get_values(keys=['c-bytes', 'a-json', 'b-text'])

    assert {key: (record.value, record.content_type) for key, record in records.items()} == {
        'c-bytes': (b'\x00\x01binary', 'application/octet-stream'),
        'a-json': (b'{"nested": [1, 2]}', 'application/json'),
        'b-text': (b'plain text', 'text/plain'),
    }
    assert all(record.size is not None for record in records.values())
    get_session.assert_called_once()


async def test_list_keys_checks_prefix_case(kvs_backend: SqlKeyValueStoreBackend) -> None:
    """`list_keys` matches the prefix case-sensitively, and a page left without a match still has a next page."""
    for key in ['Item-1', 'item-1', 'item-2', 'item-3']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')

    first_page = await kvs_backend.list_keys(prefix='Item-', limit=1)
    second_page = await kvs_backend.list_keys(
        prefix='Item-', exclusive_start_key=first_page.next_exclusive_start_key, limit=1
    )

    assert [item.key for item in first_page.items] == ['Item-1']
    assert second_page.items == []
    assert second_page.count == 0
    assert second_page.next_exclusive_start_key is not None
