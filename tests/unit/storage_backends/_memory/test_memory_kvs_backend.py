from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_backends import FileSystemStorageBackend, MemoryStorageBackend

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_backends._memory import MemoryKeyValueStoreBackend


def circular_dict() -> dict[str, Any]:
    value: dict[str, Any] = {}
    value['self'] = value
    return value


@pytest.fixture
async def kvs_backend() -> AsyncGenerator[MemoryKeyValueStoreBackend, None]:
    """Fixture that provides a fresh memory key-value store backend for each test."""
    backend = await MemoryStorageBackend().create_kvs_backend(name='test-kvs')
    yield backend
    await backend.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""

    # Create KVS and add data
    kvs_backend1 = await MemoryStorageBackend().create_kvs_backend(
        name='test-purge-kvs',
    )
    await kvs_backend1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_backend1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen with same storage backend instance
    kvs_backend2 = await MemoryStorageBackend().create_kvs_backend(
        name='test-purge-kvs',
    )

    # Verify value was purged (memory storage specific behavior)
    record = await kvs_backend2.get_value(key='test-key')
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


@pytest.mark.parametrize(
    'value',
    [
        pytest.param(circular_dict(), id='circular reference'),
        pytest.param({'nested': {(1, 2): 'tuple key'}}, id='tuple key'),
    ],
)
async def test_set_value_rejects_what_file_system_rejects(
    kvs_backend: MemoryKeyValueStoreBackend,
    tmp_path: Path,
    value: dict[str, Any],
) -> None:
    """A JSON value the file-system backend can't serialize raises the same error and stores nothing."""
    configuration = Configuration(storage_dir=str(tmp_path))
    fs_backend = await FileSystemStorageBackend().create_kvs_backend(configuration=configuration)

    with pytest.raises((TypeError, ValueError)) as fs_error:
        await fs_backend.set_value(key='test-key', value=value)

    with pytest.raises(fs_error.type, match=re.escape(str(fs_error.value))):
        await kvs_backend.set_value(key='test-key', value=value)

    assert await kvs_backend.get_value(key='test-key') is None


async def test_set_value_serializes_like_file_system(kvs_backend: MemoryKeyValueStoreBackend, tmp_path: Path) -> None:
    """A JSON value with types JSON doesn't support is stored as the file-system backend reads it back."""
    value = {'created_at': datetime(2026, 1, 1, tzinfo=UTC), 1: 'int key'}
    configuration = Configuration(storage_dir=str(tmp_path))
    fs_backend = await FileSystemStorageBackend().create_kvs_backend(configuration=configuration)

    await fs_backend.set_value(key='test-key', value=value)
    await kvs_backend.set_value(key='test-key', value=value)

    fs_record = await fs_backend.get_value(key='test-key')
    memory_record = await kvs_backend.get_value(key='test-key')
    assert fs_record is not None
    assert memory_record is not None
    assert memory_record.value == fs_record.value == {'created_at': '2026-01-01 00:00:00+00:00', '1': 'int key'}
