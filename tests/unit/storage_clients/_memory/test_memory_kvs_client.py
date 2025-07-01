from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients._memory import MemoryKeyValueStoreClient


@pytest.fixture
async def kvs_client() -> AsyncGenerator[MemoryKeyValueStoreClient, None]:
    """Fixture that provides a fresh memory key-value store client for each test."""
    client = await MemoryStorageClient().create_kvs_client(name='test_kvs')
    yield client
    await client.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""
    configuration = Configuration(purge_on_start=True)

    # Create KVS and add data
    kvs_client1 = await MemoryStorageClient().create_kvs_client(
        name='test_purge_kvs',
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen with same storage client instance
    kvs_client2 = await MemoryStorageClient().create_kvs_client(
        name='test_purge_kvs',
        configuration=configuration,
    )

    # Verify value was purged (memory storage specific behavior)
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_memory_metadata_updates(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await kvs_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_client.get_value(key='nonexistent')

    # Verify timestamps (memory-specific behavior)
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_client.set_value(key='test', value='test-value')

    # Verify timestamps were updated
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read
