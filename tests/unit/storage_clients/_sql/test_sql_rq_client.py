from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee import Request
from crawlee.configuration import Configuration
from crawlee.storage_clients import SqlStorageClient
from crawlee.storage_clients._sql._db_models import RequestDb, RequestQueueMetadataDb
from crawlee.storage_clients.models import RequestQueueMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy import Connection

    from crawlee.storage_clients._sql import SqlRequestQueueClient


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def rq_client(
    configuration: Configuration,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[SqlRequestQueueClient, None]:
    """A fixture for a SQL request queue client."""
    async with SqlStorageClient() as storage_client:
        monkeypatch.setattr(storage_client, '_accessed_modified_update_interval', timedelta(seconds=0))
        client = await storage_client.create_rq_client(
            name='test-request-queue',
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
    """Test that SQL request queue client creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'

    async with SqlStorageClient(connection_string=f'sqlite+aiosqlite:///{storage_dir}') as storage_client:
        await storage_client.create_rq_client(
            name='test-request-queue',
            configuration=configuration,
        )

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'request_queues' in tables
            assert 'request_queue_records' in tables
            assert 'request_queue_state' in tables


async def test_create_tables_with_engine(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL request queue client creates tables with a pre-configured engine."""
    storage_dir = tmp_path / 'test_table.db'

    engine = create_async_engine(f'sqlite+aiosqlite:///{storage_dir}', future=True, echo=False)

    async with SqlStorageClient(engine=engine) as storage_client:
        await storage_client.create_rq_client(
            name='test-request-queue',
            configuration=configuration,
        )

        async with engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'request_queues' in tables
            assert 'request_queue_records' in tables
            assert 'request_queue_state' in tables


async def test_tables_and_metadata_record(configuration: Configuration) -> None:
    """Test that SQL request queue creates proper tables and metadata records."""
    async with SqlStorageClient() as storage_client:
        client = await storage_client.create_rq_client(
            name='test-request-queue',
            configuration=configuration,
        )

        client_metadata = await client.get_metadata()

        async with storage_client.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'request_queues' in tables
            assert 'request_queue_records' in tables
            assert 'request_queue_state' in tables

        async with client.get_session() as session:
            stmt = select(RequestQueueMetadataDb).where(RequestQueueMetadataDb.name == 'test-request-queue')
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()
            metadata = RequestQueueMetadata.model_validate(orm_metadata)
            assert metadata.id == client_metadata.id
            assert metadata.name == 'test-request-queue'

        await client.drop()


async def test_request_records_persistence(rq_client: SqlRequestQueueClient) -> None:
    """Test that all added requests are persisted and can be retrieved from the database."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_client.add_batch_of_requests(requests)

    metadata_client = await rq_client.get_metadata()

    async with rq_client.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata_client.id)
        result = await session.execute(stmt)
        db_requests = result.scalars().all()
        assert len(db_requests) == 3
    for db_request in db_requests:
        request = json.loads(db_request.data)
        assert request['url'] in ['https://example.com/1', 'https://example.com/2', 'https://example.com/3']


async def test_drop_removes_records(rq_client: SqlRequestQueueClient) -> None:
    """Test that drop removes all records from the database."""
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    metadata = await rq_client.get_metadata()
    async with rq_client.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 1

    await rq_client.drop()

    async with rq_client.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 0
        db_metadata = await session.get(RequestQueueMetadataDb, metadata.id)
        assert db_metadata is None


async def test_metadata_record_updates(rq_client: SqlRequestQueueClient) -> None:
    """Test that metadata record updates correctly after operations."""
    # Record initial timestamps
    metadata = await rq_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_client.is_empty()

    # Verify accessed timestamp was updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify modified timestamp was updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    async with rq_client.get_session() as session:
        orm_metadata = await session.get(RequestQueueMetadataDb, metadata.id)
        assert orm_metadata is not None
        assert orm_metadata.created_at == metadata.created_at
        assert orm_metadata.accessed_at == metadata.accessed_at
        assert orm_metadata.modified_at == metadata.modified_at


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same request queue."""
    async with SqlStorageClient() as storage_client:
        original_client = await storage_client.create_rq_client(
            name='persistence-test',
            configuration=configuration,
        )

        test_requests = [
            Request.from_url('https://example.com/1'),
            Request.from_url('https://example.com/2'),
        ]
        await original_client.add_batch_of_requests(test_requests)

        rq_id = (await original_client.get_metadata()).id

        # Reopen by ID and verify data persists
        reopened_client = await storage_client.create_rq_client(
            id=rq_id,
            configuration=configuration,
        )

        metadata = await reopened_client.get_metadata()
        assert metadata.total_request_count == 2

        # Fetch requests to verify they're still there
        request1 = await reopened_client.fetch_next_request()
        request2 = await reopened_client.fetch_next_request()

        assert request1 is not None
        assert request2 is not None
        assert {request1.url, request2.url} == {'https://example.com/1', 'https://example.com/2'}

        await reopened_client.drop()
