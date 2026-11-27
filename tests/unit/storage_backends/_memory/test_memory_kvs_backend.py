from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storage_backends._utils import SortedKeys

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_backends._memory import MemoryKeyValueStoreBackend

_BACKEND_MODULE = 'crawlee.storage_backends._memory._key_value_store_backend'


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
    await kvs_backend1.set_value(key='test-key', value=b'initial value', content_type='text/plain')

    # Verify value was set
    record = await kvs_backend1.get_value(key='test-key')
    assert record is not None
    assert record.value == b'initial value'

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
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify timestamps were updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_get_value_returns_copy(kvs_backend: MemoryKeyValueStoreBackend) -> None:
    """Changing a returned record doesn't change the stored one."""
    await kvs_backend.set_value(key='key', value=b'value', content_type='text/plain')

    record = await kvs_backend.get_value(key='key')
    assert record is not None
    record.value = b'changed'

    stored_record = await kvs_backend.get_value(key='key')
    assert stored_record is not None
    assert stored_record.value == b'value'


@pytest.mark.parametrize('operation', [pytest.param('purge', id='purge'), pytest.param('drop', id='drop')])
async def test_purge_or_drop_empties_the_listing(kvs_backend: MemoryKeyValueStoreBackend, operation: str) -> None:
    """A listing that continues after a purge or drop lists no keys."""
    for key in ['a', 'b']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    first_page = await kvs_backend.list_keys(limit=1)

    await getattr(kvs_backend, operation)()

    page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key)
    assert page.items == []


async def test_list_keys_follows_writes_between_pages(kvs_backend: MemoryKeyValueStoreBackend) -> None:
    """A listing sorts the keys once, sees writes between its pages and stops tracking writes after its last page."""
    original_add = SortedKeys.add

    with (
        patch(f'{_BACKEND_MODULE}.SortedKeys', wraps=SortedKeys) as sorted_keys_class,
        patch.object(SortedKeys, 'add', autospec=True, side_effect=original_add) as add,
    ):
        for key in ['c', 'a', 'b']:
            await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
        first_page = await kvs_backend.list_keys(limit=1)
        await kvs_backend.set_value(key='d', value=b'value', content_type='text/plain')
        await kvs_backend.set_value(key='c', value=b'new value', content_type='text/plain')
        await kvs_backend.delete_value(key='b')
        second_page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key)
        await kvs_backend.set_value(key='e', value=b'value', content_type='text/plain')

    assert [item.key for item in first_page.items] == ['a']
    assert [item.key for item in second_page.items] == ['c', 'd']
    assert second_page.next_exclusive_start_key is None
    assert sorted_keys_class.call_count == 1
    assert [call.args[1] for call in add.call_args_list] == ['d', 'c']
