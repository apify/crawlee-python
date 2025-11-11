from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_clients import RedisStorageClient
from crawlee.storage_clients._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis

    from crawlee.storage_clients._redis import RedisDatasetClient


@pytest.fixture
async def dataset_client(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisDatasetClient, None]:
    """A fixture for a Redis dataset client."""
    client = await RedisStorageClient(redis=redis_client).create_dataset_client(
        name='test_dataset',
    )
    yield client
    await client.drop()


async def test_base_keys_creation(dataset_client: RedisDatasetClient) -> None:
    """Test that Redis dataset client creates proper keys."""
    metadata = await dataset_client.get_metadata()
    name = await await_redis_response(dataset_client.redis.hget('datasets:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_dataset'

    dataset_id = await await_redis_response(dataset_client.redis.hget('datasets:name_to_id', 'test_dataset'))

    assert dataset_id is not None
    assert (dataset_id.decode() if isinstance(dataset_id, bytes) else dataset_id) == metadata.id

    items = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:items', '$'))
    assert items is not None
    assert len(items) == 0

    metadata_data = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:metadata'))

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id  # type: ignore[unreachable] # py-json typing is broken


async def test_record_and_content_verification(dataset_client: RedisDatasetClient) -> None:
    """Test that data is properly persisted to Redis with correct content."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    # Verify metadata record
    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 1
    assert metadata.created_at is not None
    assert metadata.modified_at is not None
    assert metadata.accessed_at is not None

    # Verify records in Redis
    all_items = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:items', '$'))

    assert all_items is not None
    assert len(all_items) == 1

    # Verify actual file content
    assert all_items[0] == item

    # Test multiple records
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    all_items = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:items', '$'))
    assert all_items is not None
    assert len(all_items) == 4


async def test_drop_removes_records(dataset_client: RedisDatasetClient) -> None:
    """Test that dropping a dataset removes all records from Redis."""
    await dataset_client.push_data({'test': 'data'})

    metadata = await dataset_client.get_metadata()
    name = await await_redis_response(dataset_client.redis.hget('datasets:id_to_name', metadata.id))
    dataset_id = await await_redis_response(dataset_client.redis.hget('datasets:name_to_id', 'test_dataset'))
    items = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:items', '$'))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_dataset'
    assert dataset_id is not None
    assert (dataset_id.decode() if isinstance(dataset_id, bytes) else dataset_id) == metadata.id
    assert items is not None
    assert len(items) == 1

    # Drop the dataset
    await dataset_client.drop()

    # Verify removal of all records
    name_after_drop = await await_redis_response(dataset_client.redis.hget('datasets:id_to_name', metadata.id))
    dataset_id_after_drop = await await_redis_response(dataset_client.redis.hget('datasets:name_to_id', 'test_dataset'))
    items_after_drop = await await_redis_response(dataset_client.redis.json().get('datasets:test_dataset:items', '$'))

    assert name_after_drop is None
    assert dataset_id_after_drop is None
    assert items_after_drop is None


async def test_metadata_record_updates(dataset_client: RedisDatasetClient) -> None:
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
