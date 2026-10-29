from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_backends import MemoryStorageBackend

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_backends._memory import MemoryKeyValueStoreBackend


@pytest.fixture
async def kvs_backend() -> AsyncGenerator[MemoryKeyValueStoreBackend, None]:
    """Fixture that provides a fresh memory key-value store backend for each test."""
    backend = await MemoryStorageBackend().create_kvs_backend(name='test-kvs')
    yield backend
    await backend.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""

    # Create KVS and add data
    kvs_client1 = await MemoryStorageBackend().create_kvs_backend(
        name='test-purge-kvs',
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen with same storage backend instance
    kvs_client2 = await MemoryStorageBackend().create_kvs_backend(
        name='test-purge-kvs',
    )

    # Verify value was purged (memory storage specific behavior)
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_memory_metadata_updates(kvs_backend: MemoryKeyValueStoreBackend) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await kvs_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_backend.get_value(key='nonexistent')

    # Verify timestamps (memory-specific behavior)
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_backend.set_value(key='test', value='test-value')

    # Verify timestamps were updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_iterate_keys_skips_record_deleted_during_iteration(kvs_backend: MemoryKeyValueStoreBackend) -> None:
    """Test that a record deleted while `iterate_keys` is suspended is skipped."""
    await kvs_backend.set_value(key='key1', value='a')
    await kvs_backend.set_value(key='key2', value='b')

    collected_keys = []
    async for metadata in kvs_backend.iterate_keys():
        if metadata.key == 'key1':
            await kvs_backend.delete_value(key='key2')
        collected_keys.append(metadata.key)

    assert collected_keys == ['key1']
