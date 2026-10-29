from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import RedisError

from crawlee.storage_backends import RedisStorageBackend
from crawlee.storage_backends._redis._utils import await_redis_response

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fakeredis import FakeAsyncRedis

    from crawlee.storage_backends._redis import RedisDatasetBackend


@pytest.fixture
async def dataset_backend(
    redis_client: FakeAsyncRedis,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[RedisDatasetBackend, None]:
    """A fixture for a Redis dataset backend."""
    backend = await RedisStorageBackend(redis=redis_client).create_dataset_backend(
        name='test_dataset',
    )
    yield backend
    await backend.drop()


async def test_base_keys_creation(dataset_backend: RedisDatasetBackend) -> None:
    """Test that Redis dataset backend creates proper keys."""
    metadata = await dataset_backend.get_metadata()
    name = await await_redis_response(dataset_backend.redis.hget('datasets:id_to_name', metadata.id))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_dataset'

    dataset_id = await await_redis_response(dataset_backend.redis.hget('datasets:name_to_id', 'test_dataset'))

    assert dataset_id is not None
    assert (dataset_id.decode() if isinstance(dataset_id, bytes) else dataset_id) == metadata.id

    items = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:items', '$'))
    assert items is not None
    assert isinstance(items, list)
    assert len(items) == 0

    metadata_data = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:metadata'))

    assert isinstance(metadata_data, dict)
    assert metadata_data['id'] == metadata.id


async def test_record_and_content_verification(dataset_backend: RedisDatasetBackend) -> None:
    """Test that data is properly persisted to Redis with correct content."""
    item = {'key': 'value', 'number': 42}
    await dataset_backend.push_data(item)

    # Verify metadata record
    metadata = await dataset_backend.get_metadata()
    assert metadata.item_count == 1
    assert metadata.created_at is not None
    assert metadata.modified_at is not None
    assert metadata.accessed_at is not None

    # Verify records in Redis
    all_items = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:items', '$'))

    assert all_items is not None
    assert isinstance(all_items, list)
    assert len(all_items) == 1

    # Verify actual file content
    assert all_items[0] == item

    # Test multiple records
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_backend.push_data(items)

    all_items = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:items', '$'))
    assert all_items is not None
    assert isinstance(all_items, list)
    assert len(all_items) == 4


async def test_drop_removes_records(dataset_backend: RedisDatasetBackend) -> None:
    """Test that dropping a dataset removes all records from Redis."""
    await dataset_backend.push_data({'test': 'data'})

    metadata = await dataset_backend.get_metadata()
    name = await await_redis_response(dataset_backend.redis.hget('datasets:id_to_name', metadata.id))
    dataset_id = await await_redis_response(dataset_backend.redis.hget('datasets:name_to_id', 'test_dataset'))
    items = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:items', '$'))

    assert name is not None
    assert (name.decode() if isinstance(name, bytes) else name) == 'test_dataset'
    assert dataset_id is not None
    assert (dataset_id.decode() if isinstance(dataset_id, bytes) else dataset_id) == metadata.id
    assert items is not None
    assert isinstance(items, list)
    assert len(items) == 1

    # Drop the dataset
    await dataset_backend.drop()

    # Verify removal of all records
    name_after_drop = await await_redis_response(dataset_backend.redis.hget('datasets:id_to_name', metadata.id))
    dataset_id_after_drop = await await_redis_response(
        dataset_backend.redis.hget('datasets:name_to_id', 'test_dataset')
    )
    items_after_drop = await await_redis_response(dataset_backend.redis.json().get('datasets:test_dataset:items', '$'))

    assert name_after_drop is None
    assert dataset_id_after_drop is None
    assert items_after_drop is None


async def test_drop_preserves_sibling_index_entries(
    dataset_backend: RedisDatasetBackend,
    redis_client: FakeAsyncRedis,
) -> None:
    """Test that dropping a dataset does not wipe the shared index entries of other datasets."""
    storage_backend = RedisStorageBackend(redis=redis_client)
    sibling_backend = await storage_backend.create_dataset_backend(name='sibling_dataset')
    sibling_metadata = await sibling_backend.get_metadata()

    await dataset_backend.drop()

    # The sibling's entries in the shared id_to_name and name_to_id hashes must remain intact.
    sibling_name = await await_redis_response(redis_client.hget('datasets:id_to_name', sibling_metadata.id))
    sibling_id = await await_redis_response(redis_client.hget('datasets:name_to_id', 'sibling_dataset'))

    assert sibling_name is not None
    assert (sibling_name.decode() if isinstance(sibling_name, bytes) else sibling_name) == 'sibling_dataset'
    assert sibling_id is not None
    assert (sibling_id.decode() if isinstance(sibling_id, bytes) else sibling_id) == sibling_metadata.id

    # Opening the sibling by ID must still work.
    reopened = await storage_backend.create_dataset_backend(id=sibling_metadata.id)
    assert (await reopened.get_metadata()).id == sibling_metadata.id


async def test_metadata_record_updates(dataset_backend: RedisDatasetBackend) -> None:
    """Test that metadata record is updated correctly after operations."""
    # Record initial timestamps
    metadata = await dataset_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await dataset_backend.get_data()

    # Verify timestamps
    metadata = await dataset_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_get = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at
    await dataset_backend.push_data({'new': 'item'})

    # Verify timestamps again
    metadata = await dataset_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_get


async def test_error_handling_on_push_failure(dataset_backend: RedisDatasetBackend) -> None:
    """Test that push_data properly handles Redis errors and retries."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=RedisError('connection lost'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(dataset_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(RedisError),
    ):
        await dataset_backend.push_data({'test': 'data'})

    # Verify that retry logic was attempted
    assert mock_sleep.call_count == 2  # Assuming default max_attempts=3


async def test_push_data_does_not_retry_on_unexpected_exception(dataset_backend: RedisDatasetBackend) -> None:
    """Test that push_data does not retry on unexpected exceptions."""
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(side_effect=ValueError('unexpected error'))

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        patch.object(dataset_backend.redis, 'pipeline', return_value=mock_pipeline_ctx),
        pytest.raises(ValueError, match='unexpected error'),
    ):
        await dataset_backend.push_data({'test': 'data'})

    # Verify that retry logic was not attempted
    assert mock_sleep.call_count == 0
