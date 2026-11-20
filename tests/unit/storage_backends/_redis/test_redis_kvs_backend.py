from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fakeredis import FakeAsyncRedis
from redis.asyncio.client import Pipeline
from redis.exceptions import RedisError

from crawlee.configuration import Configuration
from crawlee.storage_backends import RedisStorageBackend
from crawlee.storage_backends._redis import RedisKeyValueStoreBackend
from crawlee.storage_backends._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def kvs_backend(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisKeyValueStoreBackend, None]:
    """A fixture for a Redis KVS backend."""
    backend = await RedisStorageBackend(redis=redis_client).create_kvs_backend(
        name='test_kvs',
    )
    yield backend
    await backend.drop()


async def test_base_keys_creation(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that Redis KVS backend creates proper keys."""
    metadata = await kvs_backend.get_metadata()
    name = await await_redis_response(kvs_backend.redis.hget('key_value_stores:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_kvs'

    kvs_id = await await_redis_response(kvs_backend.redis.hget('key_value_stores:name_to_id', 'test_kvs'))

    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id

    metadata_data = await await_redis_response(kvs_backend.redis.json().get('key_value_stores:test_kvs:metadata'))

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id


async def test_value_record_creation_and_content(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that values are properly persisted to records with correct content and metadata."""
    test_key = 'test-key'
    test_value = b'Hello, world!'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain; charset=utf-8')

    # Check if the records were created
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'
    record_exists = await await_redis_response(kvs_backend.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_backend.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Check record content
    content = await await_redis_response(kvs_backend.redis.hget(records_key, test_key))
    assert content == test_value

    # Check record metadata
    record_metadata = await await_redis_response(kvs_backend.redis.hget(records_items_metadata, test_key))
    assert record_metadata is not None
    assert isinstance(record_metadata, (str, bytes))
    metadata = json.loads(record_metadata)

    # Check record metadata
    assert metadata['key'] == test_key
    assert metadata['contentType'] == 'text/plain; charset=utf-8'
    assert metadata['size'] == len(test_value)

    # Verify retrieval works correctly
    check_value = await kvs_backend.get_value(key=test_key)
    assert check_value is not None
    assert check_value.value == test_value


async def test_binary_data_persistence(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='application/octet-stream')

    # Verify binary file exists
    record_exists = await await_redis_response(kvs_backend.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_backend.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Verify binary content is preserved
    content = await await_redis_response(kvs_backend.redis.hget(records_key, test_key))
    assert content == test_value

    # Verify retrieval works correctly
    record = await kvs_backend.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'


async def test_records_deletion_on_value_delete(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that deleting a value removes its records from Redis."""
    test_key = 'test-delete'
    test_value = b'Delete me'
    records_key = 'key_value_stores:test_kvs:items'
    records_items_metadata = 'key_value_stores:test_kvs:metadata_items'

    # Set a value
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain')

    # Verify records exist
    record_exists = await await_redis_response(kvs_backend.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_backend.redis.hexists(records_items_metadata, test_key))
    assert record_exists is True
    assert metadata_exists is True

    # Delete the value
    await kvs_backend.delete_value(key=test_key)

    # Verify files were deleted
    record_exists = await await_redis_response(kvs_backend.redis.hexists(records_key, test_key))
    metadata_exists = await await_redis_response(kvs_backend.redis.hexists(records_items_metadata, test_key))
    assert record_exists is False
    assert metadata_exists is False


async def test_drop_removes_keys(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """`drop` removes the store and all its keys from Redis."""
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    metadata = await kvs_backend.get_metadata()
    name = await await_redis_response(kvs_backend.redis.hget('key_value_stores:id_to_name', metadata.id))
    kvs_id = await await_redis_response(kvs_backend.redis.hget('key_value_stores:name_to_id', 'test_kvs'))
    items = await await_redis_response(kvs_backend.redis.hgetall('key_value_stores:test_kvs:items'))
    metadata_items = await await_redis_response(kvs_backend.redis.hgetall('key_value_stores:test_kvs:metadata_items'))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_kvs'
    assert kvs_id is not None
    assert (kvs_id.decode() if isinstance(kvs_id, bytes) else kvs_id) == metadata.id
    assert items is not None
    assert items != {}
    assert metadata_items is not None
    assert metadata_items != {}

    # Drop the store
    await kvs_backend.drop()

    name = await await_redis_response(kvs_backend.redis.hget('key_value_stores:id_to_name', metadata.id))
    kvs_id = await await_redis_response(kvs_backend.redis.hget('key_value_stores:name_to_id', 'test_kvs'))
    items = await await_redis_response(kvs_backend.redis.hgetall('key_value_stores:test_kvs:items'))
    metadata_items = await await_redis_response(kvs_backend.redis.hgetall('key_value_stores:test_kvs:metadata_items'))
    assert name is None
    assert kvs_id is None
    assert items == {}
    assert metadata_items == {}
    assert await await_redis_response(kvs_backend.redis.exists('key_value_stores:test_kvs:keys')) == 0


async def test_metadata_record_updates(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that read/write operations properly update metadata file timestamps."""
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


async def test_error_handling_on_set_failure(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that set_value properly handles Redis errors and retries."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=RedisError('connection lost'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(kvs_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(RedisError),
    ):
        await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_set_value_does_not_retry_on_unexpected_exception(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """Test that set_value does not retry on unexpected exceptions."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=ValueError('unexpected error'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(kvs_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(ValueError, match='unexpected error'),
    ):
        await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0


async def test_get_values_reads_records_in_single_transaction(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """`get_values` reads the metadata and the values in a single transaction instead of `get_value` per key."""
    await kvs_backend.set_value(key='a-json', value=b'{"nested": [1, 2]}', content_type='application/json')
    await kvs_backend.set_value(key='b-text', value=b'plain text', content_type='text/plain')
    await kvs_backend.set_value(key='c-bytes', value=b'\x00\x01binary', content_type='application/octet-stream')

    with (
        patch.object(kvs_backend.redis, 'pipeline', wraps=kvs_backend.redis.pipeline) as pipeline,
        patch.object(kvs_backend, 'get_value', side_effect=AssertionError('get_value must not be called')),
    ):
        records = await kvs_backend.get_values(keys=['c-bytes', 'a-json', 'b-text'])

    assert {key: (record.value, record.content_type) for key, record in records.items()} == {
        'c-bytes': (b'\x00\x01binary', 'application/octet-stream'),
        'a-json': (b'{"nested": [1, 2]}', 'application/json'),
        'b-text': (b'plain text', 'text/plain'),
    }
    pipeline.assert_called_once()


async def test_get_values_skips_missing_records(
    kvs_backend: RedisKeyValueStoreBackend, caplog: pytest.LogCaptureFixture
) -> None:
    """`get_values` silently leaves out a key without a record and a record whose value is gone."""
    await kvs_backend.set_value(key='kept', value=b'a', content_type='text/plain')
    await kvs_backend.set_value(key='removed', value=b'b', content_type='text/plain')
    await await_redis_response(kvs_backend.redis.hdel(kvs_backend._items_key, 'removed'))

    with caplog.at_level('WARNING'):
        records = await kvs_backend.get_values(keys=['kept', 'removed', 'nonexistent'])

    assert {key: record.value for key, record in records.items()} == {'kept': b'a'}
    assert 'missing' not in caplog.text


async def test_get_value_without_record_logs_nothing(
    kvs_backend: RedisKeyValueStoreBackend, caplog: pytest.LogCaptureFixture
) -> None:
    """`get_value` returns `None` for a key without a record and logs no warning."""
    with caplog.at_level('WARNING'):
        record = await kvs_backend.get_value(key='nonexistent')

    assert record is None
    assert caplog.records == []


async def test_get_values_without_keys(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """`get_values` returns no records for an empty list of keys."""
    await kvs_backend.set_value(key='key', value=b'a', content_type='text/plain')

    assert await kvs_backend.get_values(keys=[]) == {}


async def test_list_keys_reads_only_the_requested_page(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """`list_keys` reads the metadata of the keys on the page, not of the whole store."""
    for i in range(5):
        await kvs_backend.set_value(key=f'key{i}', value=b'value', content_type='text/plain')

    with (
        patch.object(Pipeline, 'hmget', autospec=True, side_effect=Pipeline.hmget) as hmget,
        patch.object(kvs_backend.redis, 'hgetall', side_effect=AssertionError('hgetall must not be called')),
    ):
        page = await kvs_backend.list_keys(exclusive_start_key='key0', limit=2)

    assert [item.key for item in page.items] == ['key1', 'key2']
    assert page.next_exclusive_start_key == 'key2'
    assert [call.args[2] for call in hmget.call_args_list] == [['key1', 'key2']]


async def test_list_keys_skips_key_without_metadata(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """A key whose metadata is gone is left out of the page and its count, and the next page starts after it."""
    for key in ['a', 'b', 'c']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    await await_redis_response(kvs_backend.redis.hdel(kvs_backend._metadata_items_key, 'b'))

    page = await kvs_backend.list_keys(limit=2)

    assert [item.key for item in page.items] == ['a']
    assert page.count == 1
    assert page.next_exclusive_start_key == 'b'


async def test_keys_index_follows_the_records(kvs_backend: RedisKeyValueStoreBackend) -> None:
    """The sorted set with the keys gains a key when its record is set and loses it when the record is deleted."""
    keys_index = 'key_value_stores:test_kvs:keys'
    await kvs_backend.set_value(key='kept', value=b'a', content_type='text/plain')
    await kvs_backend.set_value(key='removed', value=b'b', content_type='text/plain')

    await kvs_backend.delete_value(key='removed')

    assert await await_redis_response(kvs_backend.redis.zrange(keys_index, 0, -1)) == [b'kept']

    await kvs_backend.purge()

    assert await await_redis_response(kvs_backend.redis.exists(keys_index)) == 0


async def test_open_indexes_keys_of_store_without_index(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """Opening a store whose records were written before the keys index existed makes its keys listable."""
    storage_backend = RedisStorageBackend(redis=redis_client)
    kvs_backend = await storage_backend.create_kvs_backend(name='unindexed_kvs')
    await kvs_backend.set_value(key='key1', value=b'a', content_type='text/plain')
    await kvs_backend.set_value(key='key2', value=b'b', content_type='text/plain')
    await await_redis_response(redis_client.delete(kvs_backend._keys_key))

    reopened_backend = await storage_backend.create_kvs_backend(name='unindexed_kvs')

    page = await reopened_backend.list_keys()
    assert [item.key for item in page.items] == ['key1', 'key2']

    await reopened_backend.drop()


async def test_decoding_redis_client_is_rejected(suppress_user_warning: None) -> None:  # noqa: ARG001
    """Test that reading values through a Redis client created with `decode_responses=True` raises a clear error."""
    storage_backend = RedisStorageBackend(redis=FakeAsyncRedis(decode_responses=True))
    kvs_backend = await storage_backend.create_kvs_backend(name='decoding_kvs')
    await kvs_backend.set_value(key='key', value=b'value', content_type='text/plain')

    with pytest.raises(TypeError, match='decode_responses'):
        await kvs_backend.get_value(key='key')

    with pytest.raises(TypeError, match='decode_responses'):
        await kvs_backend.get_values(keys=['key'])


async def test_lost_race_uses_existing_id(
    redis_client: FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A client that loses the race to create a store opens it under the ID of the store that won."""
    configuration = Configuration(purge_on_start=False)
    storage_backend = RedisStorageBackend(redis=redis_client)
    winner = await storage_backend.create_kvs_backend(name='raced', configuration=configuration)
    winner_id = (await winner.get_metadata()).id

    original_lookup = RedisKeyValueStoreBackend._get_metadata_by_name

    async def lookup(name: str, redis: FakeAsyncRedis, *, with_wait: bool = False) -> dict | None:
        # The first lookup comes before the other client created the store, so it finds nothing.
        if lookup_mock.await_count == 1:
            return None
        return await original_lookup(name, redis, with_wait=with_wait)

    lookup_mock = AsyncMock(side_effect=lookup)
    monkeypatch.setattr(RedisKeyValueStoreBackend, '_get_metadata_by_name', staticmethod(lookup_mock))
    loser = await storage_backend.create_kvs_backend(name='raced', configuration=configuration)
    await loser.drop()

    assert await await_redis_response(redis_client.hget('key_value_stores:id_to_name', winner_id)) is None


async def test_lost_race_without_metadata_raises(
    redis_client: FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """A client that loses the race to create a store raises when the metadata of the store that won never appears."""
    configuration = Configuration(purge_on_start=False)
    storage_backend = RedisStorageBackend(redis=redis_client)
    await storage_backend.create_kvs_backend(name='raced', configuration=configuration)

    monkeypatch.setattr(RedisKeyValueStoreBackend, '_get_metadata_by_name', staticmethod(AsyncMock(return_value=None)))

    with pytest.raises(RuntimeError, match=r'did not appear in time'):
        await storage_backend.create_kvs_backend(name='raced', configuration=configuration)
