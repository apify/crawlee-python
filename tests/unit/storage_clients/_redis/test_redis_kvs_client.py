from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import RedisError

from crawlee.storage_clients import RedisStorageClient
from crawlee.storage_clients._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from fakeredis import FakeAsyncRedis

    from crawlee.storage_clients._redis import RedisKeyValueStoreClient


@pytest.fixture
async def kvs_client(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisKeyValueStoreClient, None]:
    """A fixture for a Redis KVS client."""
    client = await RedisStorageClient(redis=redis_client).create_kvs_client(
        name='test_kvs',
    )
    yield client
    await client.drop()


async def test_base_keys_creation(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that Redis KVS client creates proper keys."""
    metadata = await kvs_client.get_metadata()
    name = await await_redis_response(kvs_client.redis.hget('key_value_stores:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_kvs'

    kvs_id = await await_redis_response(kvs_client.redis.hget('key_value_stores:name_to_id', 'test_kvs'))

    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id

    metadata_data = await await_redis_response(kvs_client.redis.json().get('key_value_stores:test_kvs:metadata'))

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id


async def test_value_record_creation_and_content(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that values are properly persisted to records with correct content and metadata."""
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Check if the records were created
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'
    record_exists = await await_redis_response(kvs_client.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_client.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Check record content
    content = await await_redis_response(kvs_client.redis.hget(records_key, test_key))
    content = content.decode() if isinstance(content, bytes) else content
    assert content == test_value

    # Check record metadata
    record_metadata = await await_redis_response(kvs_client.redis.hget(records_items_metadata, test_key))
    assert record_metadata is not None
    assert isinstance(record_metadata, (str, bytes))
    metadata = json.loads(record_metadata)

    # Check record metadata
    assert metadata['key'] == test_key
    assert metadata['content_type'] == 'text/plain; charset=utf-8'
    assert metadata['size'] == len(test_value.encode('utf-8'))

    # Verify retrieval works correctly
    check_value = await kvs_client.get_value(key=test_key)
    assert check_value is not None
    assert check_value.value == test_value


async def test_binary_data_persistence(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Verify binary file exists
    record_exists = await await_redis_response(kvs_client.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_client.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Verify binary content is preserved
    content = await await_redis_response(kvs_client.redis.hget(records_key, test_key))
    assert content == test_value

    # Verify retrieval works correctly
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'


async def test_json_serialization_to_record(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that JSON objects are properly serialized to records."""
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_client.set_value(key=test_key, value=test_value)

    # Check if record content is valid JSON
    records_key = 'key_value_stores:test_kvs:items'
    record = await await_redis_response(kvs_client.redis.hget(records_key, test_key))
    assert record is not None
    assert isinstance(record, (str, bytes))
    assert json.loads(record) == test_value


async def test_records_deletion_on_value_delete(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that deleting a value removes its records from Redis."""
    test_key = 'test-delete'
    test_value = 'Delete me'
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'

    # Set a value
    await kvs_client.set_value(key=test_key, value=test_value)

    # Verify records exist
    record_exists = await await_redis_response(kvs_client.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_client.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Delete the value
    await kvs_client.delete_value(key=test_key)

    # Verify files were deleted
    record_exists = await await_redis_response(kvs_client.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_client.redis.hexists(records_items_metadata, test_key))
    assert record_exists is False
    assert metadata_exists is False


async def test_drop_removes_keys(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that drop removes the entire store directory from disk."""
    await kvs_client.set_value(key='test', value='test-value')

    metadata = await kvs_client.get_metadata()
    name = await await_redis_response(kvs_client.redis.hget('key_value_stores:id_to_name', metadata.id))
    kvs_id = await await_redis_response(kvs_client.redis.hget('key_value_stores:name_to_id', 'test_kvs'))
    items = await await_redis_response(kvs_client.redis.hgetall('key_value_stores:test_kvs:items'))
    metadata_items = await await_redis_response(kvs_client.redis.hgetall('key_value_stores:test_kvs:metadata_items'))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_kvs'
    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id
    assert items is not None
    assert items != {}
    assert metadata_items is not None
    assert metadata_items != {}

    # Drop the store
    await kvs_client.drop()

    name = await await_redis_response(kvs_client.redis.hget('key_value_stores:id_to_name', metadata.id))
    kvs_id = await await_redis_response(kvs_client.redis.hget('key_value_stores:name_to_id', 'test_kvs'))
    items = await await_redis_response(kvs_client.redis.hgetall('key_value_stores:test_kvs:items'))
    metadata_items = await await_redis_response(kvs_client.redis.hgetall('key_value_stores:test_kvs:metadata_items'))
    assert name is None
    assert kvs_id is None
    assert items == {}
    assert metadata_items == {}


async def test_metadata_record_updates(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that read/write operations properly update metadata file timestamps."""
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


async def test_error_handling_on_set_failure(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that set_value properly handles Redis errors and retries."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=RedisError('connection lost'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(kvs_client.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(RedisError),
    ):
        await kvs_client.set_value(key='test', value='test-value')

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_set_value_does_not_retry_on_unexpected_exception(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that set_value does not retry on unexpected exceptions."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=ValueError('unexpected error'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(kvs_client.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(ValueError, match='unexpected error'),
    ):
        await kvs_client.set_value(key='test', value='test-value')

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0


@pytest.fixture
def hmget_calls(kvs_client: RedisKeyValueStoreClient) -> Iterator[list[list[str]]]:
    """Record the keys of every Redis `hmget` call made through the client, while still performing the call."""
    calls: list[list[str]] = []
    original_hmget = kvs_client.redis.hmget

    def recording_hmget(name: str, keys: list[str], *args: str) -> Any:
        calls.append(list(keys))
        return original_hmget(name, keys, *args)

    with patch.object(kvs_client.redis, 'hmget', side_effect=recording_hmget):
        yield calls


async def test_iterate_entries_reads_values_in_batches(
    kvs_client: RedisKeyValueStoreClient, hmget_calls: list[list[str]]
) -> None:
    """Test that `iterate_entries` fetches values with batched HMGET calls instead of `get_value` per key."""
    await kvs_client.set_value(key='a-json', value={'nested': [1, 2]})
    await kvs_client.set_value(key='b-text', value='plain text')
    await kvs_client.set_value(key='c-bytes', value=b'\x00\x01binary', content_type='application/octet-stream')
    await kvs_client.set_value(key='d-none', value=None)

    with patch.object(kvs_client, 'get_value', side_effect=AssertionError('get_value must not be called')):
        records = [record async for record in kvs_client.iterate_entries()]

    assert [record.key for record in records] == ['a-json', 'b-text', 'c-bytes', 'd-none']
    assert [record.value for record in records] == [{'nested': [1, 2]}, 'plain text', b'\x00\x01binary', None]
    assert records[0].content_type.startswith('application/json')
    assert records[1].content_type.startswith('text/plain')
    assert records[2].content_type == 'application/octet-stream'

    # All values fit in a single batch.
    assert hmget_calls == [['a-json', 'b-text', 'c-bytes', 'd-none']]


async def test_iterate_entries_batches_are_bounded_by_key_count(
    kvs_client: RedisKeyValueStoreClient, hmget_calls: list[list[str]]
) -> None:
    """Test that `iterate_entries` splits the HMGET calls when a batch reaches the maximum number of keys."""
    for i in range(5):
        await kvs_client.set_value(key=f'key{i}', value=f'value{i}')

    with patch.object(type(kvs_client), '_ITERATE_ENTRIES_BATCH_MAX_KEYS', 2):
        records = [record async for record in kvs_client.iterate_entries()]

    assert [(record.key, record.value) for record in records] == [(f'key{i}', f'value{i}') for i in range(5)]
    assert hmget_calls == [['key0', 'key1'], ['key2', 'key3'], ['key4']]


async def test_iterate_entries_batches_are_bounded_by_size(
    kvs_client: RedisKeyValueStoreClient, hmget_calls: list[list[str]]
) -> None:
    """Test that `iterate_entries` splits the HMGET calls by the record sizes known from the metadata.

    A record larger than the limit is still fetched, but alone in its batch.
    """
    await kvs_client.set_value(key='small1', value='ab')
    await kvs_client.set_value(key='small2', value='cd')
    await kvs_client.set_value(key='large', value='x' * 100)
    await kvs_client.set_value(key='small3', value='ef')

    with patch.object(type(kvs_client), '_ITERATE_ENTRIES_BATCH_MAX_BYTES', 10):
        records = [record async for record in kvs_client.iterate_entries()]

    assert [record.key for record in records] == ['large', 'small1', 'small2', 'small3']
    assert hmget_calls == [['large'], ['small1', 'small2', 'small3']]


async def test_iterate_entries_with_exclusive_start_key_and_limit(kvs_client: RedisKeyValueStoreClient) -> None:
    """Test that `iterate_entries` applies `exclusive_start_key` and `limit`."""
    for i in range(6):
        await kvs_client.set_value(key=f'key{i}', value=f'value{i}')

    with patch.object(kvs_client, 'get_value', side_effect=AssertionError('get_value must not be called')):
        records = [record async for record in kvs_client.iterate_entries(exclusive_start_key='key1', limit=3)]

    assert [(record.key, record.value) for record in records] == [
        ('key2', 'value2'),
        ('key3', 'value3'),
        ('key4', 'value4'),
    ]


async def test_iterate_entries_empty_store(kvs_client: RedisKeyValueStoreClient) -> None:
    records = [record async for record in kvs_client.iterate_entries()]

    assert records == []
