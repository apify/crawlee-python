from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from importlib.util import find_spec
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from redis.exceptions import RedisError

from crawlee import Request
from crawlee.storage_backends import RedisStorageBackend
from crawlee.storage_backends._redis import _backend_mixin
from crawlee.storage_backends._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis

    from crawlee.storage_backends._redis import RedisRequestQueueBackend


# The Redis RQ backend runs Lua scripts, which `fakeredis` executes only with `lupa` installed.
pytestmark = pytest.mark.skipif(find_spec('lupa') is None, reason='`lupa` is not installed')


MODULE = 'crawlee.storage_backends._redis._request_queue_backend'


async def _get_blocked_until(rq_backend: RedisRequestQueueBackend, request: Request) -> int:
    """Read the lock expiry of an in-progress request, in milliseconds."""
    in_progress = await await_redis_response(rq_backend.redis.hget(rq_backend._in_progress_key, request.unique_key))
    assert in_progress is not None
    return json.loads(in_progress)['blocked_until_timestamp']


@pytest.fixture(params=['default', 'bloom'])
async def rq_backend(
    redis_client: FakeAsyncRedis,
    request: pytest.FixtureRequest,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisRequestQueueBackend, None]:
    """A fixture for a Redis RQ backend."""
    backend = await RedisStorageBackend(redis=redis_client, queue_dedup_strategy=request.param).create_rq_backend(
        name='test_request_queue'
    )
    yield backend
    await backend.drop()


async def test_base_keys_creation(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that Redis RQ backend creates proper keys."""

    metadata = await rq_backend.get_metadata()
    name = await await_redis_response(rq_backend.redis.hget('request_queues:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_request_queue'

    kvs_id = await await_redis_response(rq_backend.redis.hget('request_queues:name_to_id', 'test_request_queue'))

    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id

    if rq_backend._dedup_strategy == 'bloom':
        added_bf = await await_redis_response(
            rq_backend.redis.exists('request_queues:test_request_queue:added_bloom_filter')
        )
        assert added_bf == 1

        handled_bf = await await_redis_response(
            rq_backend.redis.exists('request_queues:test_request_queue:handled_bloom_filter')
        )
        assert handled_bf == 1

    metadata_data = await await_redis_response(
        rq_backend.redis.json().get('request_queues:test_request_queue:metadata')
    )

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id


async def test_request_records_persistence(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that requests are properly persisted to Redis."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_backend.add_batch_of_requests(requests)

    # Verify request records are created
    request_queue_response = await await_redis_response(
        rq_backend.redis.lmpop(1, 'request_queues:test_request_queue:queue', direction='left', count=10)
    )
    assert request_queue_response is not None
    assert isinstance(request_queue_response, list)
    request_keys = request_queue_response[1]
    assert isinstance(request_keys, list)
    assert len(request_keys) == 3

    # Verify actual request file content
    requests_records_data = await await_redis_response(
        rq_backend.redis.hgetall('request_queues:test_request_queue:data')
    )
    assert isinstance(requests_records_data, dict)

    for key in request_keys:
        request_data = json.loads(requests_records_data[key])
        assert 'url' in request_data
        assert request_data['url'].startswith('https://example.com/')


async def test_drop_removes_records(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that drop removes all request records from Redis."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])

    rq_queue = 'request_queues:test_request_queue:queue'
    rq_data = 'request_queues:test_request_queue:data'
    added_bf = 'request_queues:test_request_queue:added_bloom_filter'
    handled_bf = 'request_queues:test_request_queue:handled_bloom_filter'
    pending_set = 'request_queues:test_request_queue:pending_set'
    handled_set = 'request_queues:test_request_queue:handled_set'
    metadata_key = 'request_queues:test_request_queue:metadata'

    metadata = await rq_backend.get_metadata()
    name = await await_redis_response(rq_backend.redis.hget('request_queues:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_request_queue'

    rq_id = await await_redis_response(rq_backend.redis.hget('request_queues:name_to_id', 'test_request_queue'))
    assert rq_id is not None
    assert rq_id.decode() if isinstance(rq_id, bytes) else rq_id

    rq_queue_exists = await await_redis_response(rq_backend.redis.exists(rq_queue))
    rq_data_exists = await await_redis_response(rq_backend.redis.exists(rq_data))
    metadata_exists = await await_redis_response(rq_backend.redis.exists(metadata_key))
    assert rq_queue_exists == 1
    assert rq_data_exists == 1
    assert metadata_exists == 1

    if rq_backend._dedup_strategy == 'bloom':
        added_bf_exists = await await_redis_response(rq_backend.redis.exists(added_bf))
        handled_bf_exists = await await_redis_response(rq_backend.redis.exists(handled_bf))
        assert added_bf_exists == 1
        assert handled_bf_exists == 1
    elif rq_backend._dedup_strategy == 'default':
        pending_set_exists = await await_redis_response(rq_backend.redis.exists(pending_set))
        handled_set_exists = await await_redis_response(rq_backend.redis.exists(handled_set))
        assert pending_set_exists == 1
        # No requests marked as handled
        assert handled_set_exists == 0

    # Drop the request queue
    await rq_backend.drop()

    # Verify removal of all records
    name_after_drop = await await_redis_response(rq_backend.redis.hget('request_queues:id_to_name', metadata.id))
    rq_id_after_drop = await await_redis_response(
        rq_backend.redis.hget('request_queues:name_to_id', 'test_request_queue')
    )
    rq_queue_exists = await await_redis_response(rq_backend.redis.exists(rq_queue))
    rq_data_exists = await await_redis_response(rq_backend.redis.exists(rq_data))
    metadata_exists = await await_redis_response(rq_backend.redis.exists(metadata_key))
    assert name_after_drop is None
    assert rq_id_after_drop is None
    assert rq_queue_exists == 0
    assert rq_data_exists == 0
    assert metadata_exists == 0

    if rq_backend._dedup_strategy == 'bloom':
        added_bf_exists = await await_redis_response(rq_backend.redis.exists(added_bf))
        handled_bf_exists = await await_redis_response(rq_backend.redis.exists(handled_bf))
        assert added_bf_exists == 0
        assert handled_bf_exists == 0
    elif rq_backend._dedup_strategy == 'default':
        pending_set_exists = await await_redis_response(rq_backend.redis.exists(pending_set))
        handled_set_exists = await await_redis_response(rq_backend.redis.exists(handled_set))
        assert pending_set_exists == 0
        assert handled_set_exists == 0


async def test_metadata_record_updates(rq_backend: RedisRequestQueueBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetching bumps only `accessed_at`, adding requests bumps `modified_at` too, and `created_at` never changes."""
    metadata = await rq_backend.get_metadata()
    initial_created = metadata.created_at
    initial_modified = metadata.modified_at

    # Stamp the metadata updates from a controlled clock, so the checks don't depend on the wall clock resolution.
    clock = Mock()
    monkeypatch.setattr(_backend_mixin, 'datetime', clock)

    read_time = metadata.accessed_at + timedelta(seconds=1)
    clock.now.return_value = read_time
    await rq_backend.fetch_next_request()

    # `get_metadata()` reads the record before stamping its own access, so it reports the fetch's stamp.
    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at == read_time
    assert metadata.modified_at == initial_modified

    write_time = read_time + timedelta(seconds=1)
    clock.now.return_value = write_time
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com')])

    metadata = await rq_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at == write_time
    assert metadata.modified_at == write_time


async def test_get_request(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that get_request works correctly."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    added_requests = await rq_backend.add_batch_of_requests(requests)
    assert len(added_requests.processed_requests) == 3

    for req in requests:
        fetched_request = await rq_backend.get_request(req.unique_key)
        assert fetched_request is not None
        assert fetched_request.unique_key == req.unique_key
        assert fetched_request.url == req.url

    # Test fetching a non-existent request
    non_existent = await rq_backend.get_request('non-existent-id')
    assert non_existent is None


async def test_deduplication(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that request deduplication works correctly."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_backend.add_batch_of_requests(requests)

    # Verify only unique requests are added
    metadata = await rq_backend.get_metadata()
    assert metadata.pending_request_count == 2
    assert metadata.total_request_count == 2

    # Fetch requests and verify order
    request1 = await rq_backend.fetch_next_request()
    assert request1 is not None
    assert request1 == requests[0]

    # Fetch the next request, which should skip the duplicate
    request2 = await rq_backend.fetch_next_request()
    assert request2 is not None
    assert request2 == requests[2]

    # Verify no more requests are available
    request3 = await rq_backend.fetch_next_request()
    assert request3 is None


async def test_error_handling_on_get_metadata_failure(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that get_metadata properly handles Redis errors and retries."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=RedisError('connection lost'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(rq_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(RedisError),
    ):
        await rq_backend.get_metadata()

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_get_metadata_does_not_retry_on_unexpected_exception(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that get_metadata does not retry on unexpected exceptions."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=ValueError('unexpected error'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(rq_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(ValueError, match='unexpected error'),
    ):
        await rq_backend.get_metadata()

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0


async def test_mark_request_as_handled_concurrent_no_double_decrement(
    rq_backend: RedisRequestQueueBackend,
) -> None:
    """Test that concurrent calls to mark_request_as_handled decrement pending_request_count exactly once."""
    request = Request.from_url('https://example.com/concurrent')
    await rq_backend.add_batch_of_requests([request])

    fetched = await rq_backend.fetch_next_request()
    assert fetched is not None

    results = await asyncio.gather(
        rq_backend.mark_request_as_handled(fetched),
        rq_backend.mark_request_as_handled(fetched),
        rq_backend.mark_request_as_handled(fetched),
        rq_backend.mark_request_as_handled(fetched),
        rq_backend.mark_request_as_handled(fetched),
    )

    successful = [result for result in results if result is not None]
    assert len(successful) == 1

    metadata = await rq_backend.get_metadata()
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 1


async def test_mark_request_as_handled_restores_in_progress_on_pipeline_failure(
    rq_backend: RedisRequestQueueBackend,
) -> None:
    """Test that 'request' is restored to 'in_progress' when the pipeline fails after 'hdel'."""
    request = Request.from_url('https://example.com/restore')
    await rq_backend.add_batch_of_requests([request])

    fetched = await rq_backend.fetch_next_request()
    assert fetched is not None

    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=RedisError('connection lost'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock),
        patch.object(rq_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(RedisError),
    ):
        await rq_backend.mark_request_as_handled(fetched)

    in_progress = await await_redis_response(rq_backend.redis.hexists(rq_backend._in_progress_key, fetched.unique_key))
    assert in_progress

    metadata = await rq_backend.get_metadata()
    assert metadata.pending_request_count == 1
    assert metadata.handled_request_count == 0


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
    rq_backend: RedisRequestQueueBackend,
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

    blocked_until = await _get_blocked_until(rq_backend, request)
    earliest = int((fetch_started + expected_block_time).timestamp() * 1000)
    latest = int((fetch_finished + expected_block_time).timestamp() * 1000)
    assert earliest <= blocked_until <= latest


async def test_extend_moves_lock_expiry(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that extending a held lock moves its expiry by the given delta."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    blocked_until = await _get_blocked_until(rq_backend, request)

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is True
    assert await _get_blocked_until(rq_backend, request) == blocked_until + 600_000


async def test_extend_after_handled(rq_backend: RedisRequestQueueBackend) -> None:
    """Test that the lock of a handled request isn't extended."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    await rq_backend.mark_request_as_handled(request)

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is False


async def test_extend_lock_of_another_client(
    rq_backend: RedisRequestQueueBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a lock held by another client isn't extended."""
    await rq_backend.add_batch_of_requests([Request.from_url('https://example.com/1')])
    request = await rq_backend.fetch_next_request()
    assert request is not None
    blocked_until = await _get_blocked_until(rq_backend, request)

    monkeypatch.setattr(rq_backend, 'client_key', 'another-client')

    assert await rq_backend.extend_request_processing_time(request, timedelta(minutes=10)) is False
    assert await _get_blocked_until(rq_backend, request) == blocked_until


async def test_extend_expired_lock(rq_backend: RedisRequestQueueBackend, monkeypatch: pytest.MonkeyPatch) -> None:
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
async def test_extend_rejects_non_positive_delta(rq_backend: RedisRequestQueueBackend, delta: timedelta) -> None:
    """Test that a lock extension must be positive."""
    with pytest.raises(ValueError, match='must be positive'):
        await rq_backend.extend_request_processing_time(Request.from_url('https://example.com/1'), delta)
