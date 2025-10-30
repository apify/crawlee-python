from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_clients import RedisStorageClient
from crawlee.storage_clients._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

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
    assert metadata_data['id'] == metadata.id  # type: ignore[unreachable] # py-json typing is broken


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
