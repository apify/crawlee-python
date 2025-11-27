from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_clients import SqlStorageClient
from crawlee.storage_clients._sql._db_models import KeyValueStoreMetadataDb, KeyValueStoreRecordDb
from crawlee.storage_clients.models import KeyValueStoreMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy import Connection

    from crawlee.storage_clients._sql import SqlKeyValueStoreClient


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def kvs_client(
    configuration: Configuration,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[SqlKeyValueStoreClient, None]:
    """A fixture for a SQL key-value store client."""
    async with SqlStorageClient() as storage_client:
        monkeypatch.setattr(storage_client, '_accessed_modified_update_interval', timedelta(seconds=0))
        client = await storage_client.create_kvs_client(
            name='test-kvs',
            configuration=configuration,
        )
        monkeypatch.setattr(client, '_accessed_modified_update_interval', timedelta(seconds=0))
        yield client
        await client.drop()


# Helper function that allows you to use inspect with an asynchronous engine
def get_tables(sync_conn: Connection) -> list[str]:
    inspector = inspect(sync_conn)
    return inspector.get_table_names()


async def test_create_tables_with_connection_string(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL key-value store client creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'

    async with SqlStorageClient(connection_string=f'sqlite+aiosqlite:///{storage_dir}') as storage_client:
        await storage_client.create_kvs_client(
            name='new-kvs',
            configuration=configuration,
        )

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables


async def test_create_tables_with_engine(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL key-value store client creates tables with a pre-configured engine."""
    storage_dir = tmp_path / 'test_table.db'

    engine = create_async_engine(f'sqlite+aiosqlite:///{storage_dir}', future=True, echo=False)

    async with SqlStorageClient(engine=engine) as storage_client:
        await storage_client.create_kvs_client(
            name='new-kvs',
            configuration=configuration,
        )

        async with engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables


async def test_tables_and_metadata_record(configuration: Configuration) -> None:
    """Test that SQL key-value store creates proper tables and metadata records."""
    async with SqlStorageClient() as storage_client:
        client = await storage_client.create_kvs_client(
            name='new-kvs',
            configuration=configuration,
        )

        client_metadata = await client.get_metadata()

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'key_value_stores' in tables
            assert 'key_value_store_records' in tables

        async with client.get_session() as session:
            stmt = select(KeyValueStoreMetadataDb).where(KeyValueStoreMetadataDb.name == 'new-kvs')
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()
            metadata = KeyValueStoreMetadata.model_validate(orm_metadata)
            assert metadata.id == client_metadata.id
            assert metadata.name == 'new-kvs'

        await client.drop()


async def test_value_record_creation(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that SQL key-value store client can create a record."""
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_client.set_value(key=test_key, value=test_value)
    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.content_type == 'text/plain; charset=utf-8'
        assert record.size == len(test_value.encode('utf-8'))
        assert record.value == test_value.encode('utf-8')


async def test_binary_data_persistence(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_client.set_value(key=test_key, value=test_value)

    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.content_type == 'application/octet-stream'
        assert record.size == len(test_value)
        assert record.value == test_value

    verify_record = await kvs_client.get_value(key=test_key)
    assert verify_record is not None
    assert verify_record.value == test_value
    assert verify_record.content_type == 'application/octet-stream'


async def test_json_serialization_to_record(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that JSON objects are properly serialized to records."""
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_client.set_value(key=test_key, value=test_value)

    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert json.loads(record.value.decode('utf-8')) == test_value


async def test_record_deletion_on_value_delete(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that deleting a value removes its record from the database."""
    test_key = 'test-delete'
    test_value = 'Delete me'

    # Set a value
    await kvs_client.set_value(key=test_key, value=test_value)

    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.key == test_key
        assert record.value == test_value.encode('utf-8')

    # Delete the value
    await kvs_client.delete_value(key=test_key)

    # Verify record was deleted
    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == test_key)
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is None


async def test_drop_removes_records(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that drop removes all records from the database."""
    await kvs_client.set_value(key='test', value='test-value')

    client_metadata = await kvs_client.get_metadata()

    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == 'test')
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is not None

    # Drop the store
    await kvs_client.drop()

    async with kvs_client.get_session() as session:
        stmt = select(KeyValueStoreRecordDb).where(KeyValueStoreRecordDb.key == 'test')
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        assert record is None
        metadata = await session.get(KeyValueStoreMetadataDb, client_metadata.id)
        assert metadata is None


async def test_metadata_record_updates(kvs_client: SqlKeyValueStoreClient) -> None:
    """Test that read/write operations properly update metadata record timestamps."""
    # Record initial timestamps
    metadata = await kvs_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_client.get_value(key='nonexistent')

    # Verify accessed timestamp was updated
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_client.set_value(key='test', value='test-value')

    # Verify modified timestamp was updated
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    async with kvs_client.get_session() as session:
        orm_metadata = await session.get(KeyValueStoreMetadataDb, metadata.id)
        assert orm_metadata is not None
        assert orm_metadata.created_at == metadata.created_at
        assert orm_metadata.accessed_at == metadata.accessed_at
        assert orm_metadata.modified_at == metadata.modified_at


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same key-value store."""
    async with SqlStorageClient() as storage_client:
        original_client = await storage_client.create_kvs_client(
            name='persistence-test',
            configuration=configuration,
        )

        test_key = 'persistent-key'
        test_value = 'persistent-value'
        await original_client.set_value(key=test_key, value=test_value)

        kvs_id = (await original_client.get_metadata()).id

        # Reopen by ID and verify data persists
        reopened_client = await storage_client.create_kvs_client(
            id=kvs_id,
            configuration=configuration,
        )

        record = await reopened_client.get_value(key=test_key)
        assert record is not None
        assert record.value == test_value

        await reopened_client.drop()
