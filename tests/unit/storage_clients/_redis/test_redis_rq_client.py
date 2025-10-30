from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.storage_clients import RedisStorageClient
from crawlee.storage_clients._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis

    from crawlee.storage_clients._redis import RedisRequestQueueClient


@pytest.fixture(params=['default', 'bloom'])
async def rq_client(
    redis_client: FakeAsyncRedis,
    request: pytest.FixtureRequest,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisRequestQueueClient, None]:
    """A fixture for a Redis RQ client."""
    client = await RedisStorageClient(redis=redis_client, queue_dedup_strategy=request.param).create_rq_client(
        name='test_request_queue'
    )
    yield client
    await client.drop()


async def test_base_keys_creation(rq_client: RedisRequestQueueClient) -> None:
    """Test that Redis RQ client creates proper keys."""

    metadata = await rq_client.get_metadata()
    name = await await_redis_response(rq_client.redis.hget('request_queues:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_request_queue'

    kvs_id = await await_redis_response(rq_client.redis.hget('request_queues:name_to_id', 'test_request_queue'))

    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id

    if rq_client._dedup_strategy == 'bloom':
        added_bf = await await_redis_response(
            rq_client.redis.exists('request_queues:test_request_queue:added_bloom_filter')
        )
        assert added_bf == 1

        handled_bf = await await_redis_response(
            rq_client.redis.exists('request_queues:test_request_queue:handled_bloom_filter')
        )
        assert handled_bf == 1

    metadata_data = await await_redis_response(rq_client.redis.json().get('request_queues:test_request_queue:metadata'))

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id  # type: ignore[unreachable] # py-json typing is broken


async def test_request_records_persistence(rq_client: RedisRequestQueueClient) -> None:
    """Test that requests are properly persisted to Redis."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_client.add_batch_of_requests(requests)

    # Verify request records are created
    request_queue_response = await await_redis_response(
        rq_client.redis.lmpop(1, 'request_queues:test_request_queue:queue', direction='left', count=10)
    )
    assert request_queue_response is not None
    assert isinstance(request_queue_response, list)
    request_keys = request_queue_response[1]

    assert len(request_keys) == 3

    # Verify actual request file content
    requests_records_data = await await_redis_response(
        rq_client.redis.hgetall('request_queues:test_request_queue:data')
    )
    assert isinstance(requests_records_data, dict)

    for key in request_keys:
        request_data = json.loads(requests_records_data[key])
        assert 'url' in request_data
        assert request_data['url'].startswith('https://example.com/')


async def test_drop_removes_records(rq_client: RedisRequestQueueClient) -> None:
    """Test that drop removes all request records from Redis."""
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    rq_queue = 'request_queues:test_request_queue:queue'
    rq_data = 'request_queues:test_request_queue:data'
    added_bf = 'request_queues:test_request_queue:added_bloom_filter'
    handled_bf = 'request_queues:test_request_queue:handled_bloom_filter'
    pending_set = 'request_queues:test_request_queue:pending_set'
    handled_set = 'request_queues:test_request_queue:handled_set'
    metadata_key = 'request_queues:test_request_queue:metadata'

    metadata = await rq_client.get_metadata()
    name = await await_redis_response(rq_client.redis.hget('request_queues:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_request_queue'

    rq_id = await await_redis_response(rq_client.redis.hget('request_queues:name_to_id', 'test_request_queue'))
    assert rq_id is not None
    assert rq_id.decode() if isinstance(rq_id, bytes) else rq_id

    rq_queue_exists = await await_redis_response(rq_client.redis.exists(rq_queue))
    rq_data_exists = await await_redis_response(rq_client.redis.exists(rq_data))
    metadata_exists = await await_redis_response(rq_client.redis.exists(metadata_key))
    assert rq_queue_exists == 1
    assert rq_data_exists == 1
    assert metadata_exists == 1

    if rq_client._dedup_strategy == 'bloom':
        added_bf_exists = await await_redis_response(rq_client.redis.exists(added_bf))
        handled_bf_exists = await await_redis_response(rq_client.redis.exists(handled_bf))
        assert added_bf_exists == 1
        assert handled_bf_exists == 1
    elif rq_client._dedup_strategy == 'default':
        pending_set_exists = await await_redis_response(rq_client.redis.exists(pending_set))
        handled_set_exists = await await_redis_response(rq_client.redis.exists(handled_set))
        assert pending_set_exists == 1
        # No requests marked as handled
        assert handled_set_exists == 0

    # Drop the request queue
    await rq_client.drop()

    # Verify removal of all records
    name_after_drop = await await_redis_response(rq_client.redis.hget('request_queues:id_to_name', metadata.id))
    rq_id_after_drop = await await_redis_response(
        rq_client.redis.hget('request_queues:name_to_id', 'test_request_queue')
    )
    rq_queue_exists = await await_redis_response(rq_client.redis.exists(rq_queue))
    rq_data_exists = await await_redis_response(rq_client.redis.exists(rq_data))
    metadata_exists = await await_redis_response(rq_client.redis.exists(metadata_key))
    assert name_after_drop is None
    assert rq_id_after_drop is None
    assert rq_queue_exists == 0
    assert rq_data_exists == 0
    assert metadata_exists == 0

    if rq_client._dedup_strategy == 'bloom':
        added_bf_exists = await await_redis_response(rq_client.redis.exists(added_bf))
        handled_bf_exists = await await_redis_response(rq_client.redis.exists(handled_bf))
        assert added_bf_exists == 0
        assert handled_bf_exists == 0
    elif rq_client._dedup_strategy == 'default':
        pending_set_exists = await await_redis_response(rq_client.redis.exists(pending_set))
        handled_set_exists = await await_redis_response(rq_client.redis.exists(handled_set))
        assert pending_set_exists == 0
        assert handled_set_exists == 0


async def test_metadata_file_updates(rq_client: RedisRequestQueueClient) -> None:
    """Test that metadata file is updated correctly after operations."""
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


async def test_get_request(rq_client: RedisRequestQueueClient) -> None:
    """Test that get_request works correctly."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    added_requests = await rq_client.add_batch_of_requests(requests)
    assert len(added_requests.processed_requests) == 3

    for req in requests:
        fetched_request = await rq_client.get_request(req.unique_key)
        assert fetched_request is not None
        assert fetched_request.unique_key == req.unique_key
        assert fetched_request.url == req.url

    # Test fetching a non-existent request
    non_existent = await rq_client.get_request('non-existent-id')
    assert non_existent is None


async def test_deduplication(rq_client: RedisRequestQueueClient) -> None:
    """Test that request deduplication works correctly."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/3'),
    ]

    await rq_client.add_batch_of_requests(requests)

    # Verify only unique requests are added
    metadata = await rq_client.get_metadata()
    assert metadata.pending_request_count == 2
    assert metadata.total_request_count == 2

    # Fetch requests and verify order
    request1 = await rq_client.fetch_next_request()
    assert request1 is not None
    assert request1 == requests[0]

    # Fetch the next request, which should skip the duplicate
    request2 = await rq_client.fetch_next_request()
    assert request2 is not None
    assert request2 == requests[2]

    # Verify no more requests are available
    request3 = await rq_client.fetch_next_request()
    assert request3 is None
