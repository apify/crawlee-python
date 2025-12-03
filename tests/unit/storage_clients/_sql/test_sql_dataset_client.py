from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_clients import SqlStorageClient
from crawlee.storage_clients._sql._db_models import DatasetItemDb, DatasetMetadataDb

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy import Connection

    from crawlee.storage_clients._sql import SqlDatasetClient


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        storage_dir=str(tmp_path),
    )


# Helper function that allows you to use inspect with an asynchronous engine
def get_tables(sync_conn: Connection) -> list[str]:
    inspector = inspect(sync_conn)
    return inspector.get_table_names()


@pytest.fixture
async def dataset_client(
    configuration: Configuration,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[SqlDatasetClient, None]:
    """A fixture for a SQL dataset client."""
    async with SqlStorageClient() as storage_client:
        monkeypatch.setattr(storage_client, '_accessed_modified_update_interval', timedelta(seconds=0))
        client = await storage_client.create_dataset_client(
            name='test-dataset',
            configuration=configuration,
        )
        yield client
        await client.drop()


async def test_create_tables_with_connection_string(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL dataset client creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'

    async with SqlStorageClient(connection_string=f'sqlite+aiosqlite:///{storage_dir}') as storage_client:
        await storage_client.create_dataset_client(
            name='new-dataset',
            configuration=configuration,
        )

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'dataset_records' in tables
            assert 'datasets' in tables


async def test_create_tables_with_engine(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL dataset client creates tables with a pre-configured engine."""
    storage_dir = tmp_path / 'test_table.db'

    engine = create_async_engine(f'sqlite+aiosqlite:///{storage_dir}', future=True, echo=False)

    async with SqlStorageClient(engine=engine) as storage_client:
        await storage_client.create_dataset_client(
            name='new-dataset',
            configuration=configuration,
        )

        async with engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'dataset_records' in tables
            assert 'datasets' in tables


async def test_tables_and_metadata_record(configuration: Configuration) -> None:
    """Test that SQL dataset creates proper tables and metadata records."""
    async with SqlStorageClient() as storage_client:
        client = await storage_client.create_dataset_client(
            name='new-dataset',
            configuration=configuration,
        )

        client_metadata = await client.get_metadata()

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'dataset_records' in tables
            assert 'datasets' in tables

        async with client.get_session() as session:
            stmt = select(DatasetMetadataDb).where(DatasetMetadataDb.name == 'new-dataset')
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()
            assert orm_metadata is not None
            assert orm_metadata.id == client_metadata.id
            assert orm_metadata.name == 'new-dataset'
            assert orm_metadata.item_count == 0

        await client.drop()


async def test_record_and_content_verification(dataset_client: SqlDatasetClient) -> None:
    """Test that dataset client can push data and verify its content."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    # Verify metadata record
    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 1
    assert metadata.created_at is not None
    assert metadata.modified_at is not None
    assert metadata.accessed_at is not None

    async with dataset_client.get_session() as session:
        stmt = select(DatasetItemDb).where(DatasetItemDb.dataset_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 1
        saved_item = records[0].data
        assert saved_item == item

    # Test pushing multiple items and verify total count
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    async with dataset_client.get_session() as session:
        stmt = select(DatasetItemDb).where(DatasetItemDb.dataset_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 4


async def test_drop_removes_records(dataset_client: SqlDatasetClient) -> None:
    """Test that dropping a dataset removes all records from the database."""
    await dataset_client.push_data({'test': 'data'})

    client_metadata = await dataset_client.get_metadata()

    async with dataset_client.get_session() as session:
        stmt = select(DatasetItemDb).where(DatasetItemDb.dataset_id == client_metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 1

    # Drop the dataset
    await dataset_client.drop()

    async with dataset_client.get_session() as session:
        stmt = select(DatasetItemDb).where(DatasetItemDb.dataset_id == client_metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 0
        metadata = await session.get(DatasetMetadataDb, client_metadata.id)
        assert metadata is None


async def test_metadata_record_updates(dataset_client: SqlDatasetClient) -> None:
    """Test that metadata record is updated correctly after operations."""
    # Record initial timestamps
    metadata = await dataset_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await dataset_client.get_data()

    # Verify timestamps
    metadata = await dataset_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_get = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at
    await dataset_client.push_data({'new': 'item'})

    # Verify timestamps again
    metadata = await dataset_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_get

    # Verify metadata record is updated in db
    async with dataset_client.get_session() as session:
        orm_metadata = await session.get(DatasetMetadataDb, metadata.id)
        assert orm_metadata is not None
        orm_metadata.item_count = 1
        assert orm_metadata.created_at == initial_created
        assert orm_metadata.accessed_at == metadata.accessed_at
        assert orm_metadata.modified_at == metadata.modified_at


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same dataset."""
    async with SqlStorageClient() as storage_client:
        original_client = await storage_client.create_dataset_client(
            name='persistence-test',
            configuration=configuration,
        )

        test_data = {'test_item': 'test_value', 'id': 123}
        await original_client.push_data(test_data)

        dataset_id = (await original_client.get_metadata()).id

        reopened_client = await storage_client.create_dataset_client(
            id=dataset_id,
            configuration=configuration,
        )

        data = await reopened_client.get_data()
        assert len(data.items) == 1
        assert data.items[0] == test_data

        await reopened_client.drop()
