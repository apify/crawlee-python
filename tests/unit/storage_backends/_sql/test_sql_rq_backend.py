from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee import Request
from crawlee.configuration import Configuration
from crawlee.storage_backends import SqlStorageBackend
from crawlee.storage_backends._sql._db_models import RequestDb, RequestQueueMetadataDb
from crawlee.storage_backends.models import RequestQueueMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from sqlalchemy import Connection

    from crawlee.storage_backends._sql import SqlRequestQueueBackend

MODULE = 'crawlee.storage_backends._sql._request_queue_backend'


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def rq_backend(
    configuration: Configuration,
) -> AsyncGenerator[SqlRequestQueueBackend, None]:
    """A fixture for a SQL request queue backend."""
    async with SqlStorageBackend() as storage_backend:
        backend = await storage_backend.create_rq_backend(
            name='test-request-queue',
            configuration=configuration,
        )
        yield backend
        await backend.drop()


async def get_blocked_until(rq_backend: SqlRequestQueueBackend) -> datetime | None:
    """Read the lock expiry of the only request in the queue."""
    metadata = await rq_backend.get_metadata()
    async with rq_backend.get_session() as session:
        stmt = select(RequestDb.time_blocked_until).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        return result.scalar_one()


# Helper function that allows you to use inspect with an asynchronous engine
def get_tables(sync_conn: Connection) -> list[str]:
    inspector = inspect(sync_conn)
    return inspector.get_table_names()


async def test_create_tables_with_connection_string(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL request queue backend creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'

    async with SqlStorageBackend(connection_string=f'sqlite+aiosqlite:///{storage_dir}') as storage_backend:
        await storage_backend.create_rq_backend(
            name='test-request-queue',
            configuration=configuration,
        )

        async with storage_backend.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'request_queues' in tables
            assert 'request_queue_records' in tables
            assert 'request_queue_state' in tables


async def test_create_tables_with_engine(configuration: Configuration, tmp_path: Path) -> None:
    """Test that SQL request queue backend creates tables with a pre-configured engine."""
    storage_dir = tmp_path / 'test_table.db'

    engine = create_async_engine(f'sqlite+aiosqlite:///{storage_dir}', future=True, echo=False)

    async with SqlStorageBackend(engine=engine) as storage_backend:
        await storage_backend.create_rq_backend(
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
    async with SqlStorageBackend() as storage_backend:
        backend = await storage_backend.create_rq_backend(
            name='test-request-queue',
            configuration=configuration,
        )

        backend_metadata = await backend.get_metadata()

        async with storage_backend.engine.begin() as conn:
            tables = await conn.run_sync(get_tables)
            assert 'request_queues' in tables
            assert 'request_queue_records' in tables
            assert 'request_queue_state' in tables

        async with backend.get_session() as session:
            stmt = select(RequestQueueMetadataDb).where(RequestQueueMetadataDb.name == 'test-request-queue')
            result = await session.execute(stmt)
            orm_metadata = result.scalar_one_or_none()
            metadata = RequestQueueMetadata.model_validate(orm_metadata)
            assert metadata.id == backend_metadata.id
            assert metadata.name == 'test-request-queue'

        await backend.drop()


async def test_request_records_persistence(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that all added requests are persisted and can be retrieved from the database."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_backend.add_batch_of_requests(requests)

    metadata = await rq_backend.get_metadata()

    async with rq_backend.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        db_requests = result.scalars().all()
        assert len(db_requests) == 3
    for db_request in db_requests:
        request = json.loads(db_request.data)
        assert request['url'] in ['https://example.com/1', 'https://example.com/2', 'https://example.com/3']


async def test_drop_removes_records(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that drop removes all records from the database."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])
    metadata = await rq_backend.get_metadata()
    async with rq_backend.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 1

    await rq_backend.drop()

    async with rq_backend.get_session() as session:
        stmt = select(RequestDb).where(RequestDb.request_queue_id == metadata.id)
        result = await session.execute(stmt)
        records = result.scalars().all()
        assert len(records) == 0
        db_metadata = await session.get(RequestQueueMetadataDb, metadata.id)
        assert db_metadata is None


async def test_metadata_record_updates(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that metadata record updates correctly after operations."""
    # Record initial timestamps
    metadata = await rq_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_backend.is_empty()

    # Verify accessed timestamp was updated
    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify modified timestamp was updated
    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read

    async with rq_backend.get_session() as session:
        orm_metadata = await session.get(RequestQueueMetadataDb, metadata.id)
        assert orm_metadata is not None
        assert orm_metadata.created_at == metadata.created_at
        assert orm_metadata.accessed_at == metadata.accessed_at
        assert orm_metadata.modified_at == metadata.modified_at


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same request queue."""
    async with SqlStorageBackend() as storage_backend:
        original_backend = await storage_backend.create_rq_backend(
            name='persistence-test',
            configuration=configuration,
        )

        test_requests = [
            Request.from_url('https://example.com/1'),
            Request.from_url('https://example.com/2'),
        ]
        await original_backend.add_batch_of_requests(test_requests)

        rq_id = (await original_backend.get_metadata()).id

        # Reopen by ID and verify data persists
        reopened_backend = await storage_backend.create_rq_backend(
            id=rq_id,
            configuration=configuration,
        )

        metadata = await reopened_backend.get_metadata()
        assert metadata.total_request_count == 2

        # Fetch requests to verify they're still there
        request1 = await reopened_backend.fetch_next_request()
        request2 = await reopened_backend.fetch_next_request()

        assert request1 is not None
        assert request2 is not None
        assert {request1.url, request2.url} == {'https://example.com/1', 'https://example.com/2'}

        await reopened_backend.drop()


async def test_error_handling_on_get_metadata_failure(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that get_metadata properly handles SQL errors and retries."""
    with patch(
        'crawlee.storage_backends._sql._request_queue_backend.SqlRequestQueueBackend.get_session',
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
            await rq_backend.get_metadata()

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_get_metadata_does_not_retry_on_unexpected_exception(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that get_metadata does not retry on unexpected exceptions."""
    with patch(
        'crawlee.storage_backends._sql._request_queue_backend.SqlRequestQueueBackend.get_session',
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
            await rq_backend.get_metadata()

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0


@pytest.mark.parametrize(
    ('processing_times', 'expected_block_time'),
    [
        pytest.param([], timedelta(minutes=3), id='default'),
        pytest.param([timedelta(minutes=1)], timedelta(minutes=3), id='shorter keeps default'),
        pytest.param([timedelta(minutes=10)], timedelta(minutes=10), id='longer raises'),
        pytest.param([timedelta(minutes=10), timedelta(minutes=5)], timedelta(minutes=10), id='never lowered'),
    ],
)
async def test_block_follows_processing_time(
    rq_backend: SqlRequestQueueBackend,
    processing_times: list[timedelta],
    expected_block_time: timedelta,
) -> None:
    """Test that a fetched request is blocked for the longest of the default and the expected processing times."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    for processing_time in processing_times:
        await rq_backend.set_expected_request_processing_time(processing_time)

    fetch_started = datetime.now(UTC)
    request = await rq_backend.fetch_next_request()
    fetch_finished = datetime.now(UTC)
    assert request is not None

    blocked_until = await get_blocked_until(rq_backend)
    assert blocked_until is not None
    assert fetch_started + expected_block_time <= blocked_until <= fetch_finished + expected_block_time


async def test_extend_moves_lock_expiry(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that extending a held lock moves its expiry by the given delta."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    blocked_until = await get_blocked_until(rq_backend)
    assert blocked_until is not None

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is True
    assert await get_blocked_until(rq_backend) == blocked_until + timedelta(minutes=10)


async def test_extend_after_handled(rq_backend: SqlRequestQueueBackend) -> None:
    """Test that the lock of a handled request isn't extended."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    await rq_backend.mark_request_as_handled(request)

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is False


async def test_extend_lock_of_another_client(
    rq_backend: SqlRequestQueueBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a lock held by another client isn't extended."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    blocked_until = await get_blocked_until(rq_backend)

    monkeypatch.setattr(rq_backend, 'client_key', 'another-client')

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is False
    assert await get_blocked_until(rq_backend) == blocked_until


async def test_extend_expired_lock(rq_backend: SqlRequestQueueBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a lock that has already expired isn't extended."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None

    after_expiry = datetime.now(UTC) + timedelta(hours=1)
    monkeypatch.setattr(f'{MODULE}.datetime', Mock(now=Mock(return_value=after_expiry)))

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is False


@pytest.mark.parametrize(
    'delta',
    [
        pytest.param(timedelta(0), id='zero'),
        pytest.param(timedelta(seconds=-1), id='negative'),
    ],
)
async def test_extend_rejects_non_positive_delta(rq_backend: SqlRequestQueueBackend, delta: timedelta) -> None:
    """Test that a lock extension must be positive."""
    with pytest.raises(ValueError, match='must be positive'):
        await rq_backend.extend_request_processing_time(Request.from_url('https://example.com/1'), delta)
