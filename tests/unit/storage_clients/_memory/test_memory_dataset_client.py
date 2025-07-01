from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients._memory import MemoryDatasetClient


@pytest.fixture
async def dataset_client() -> AsyncGenerator[MemoryDatasetClient, None]:
    """Fixture that provides a fresh memory dataset client for each test."""
    client = await MemoryStorageClient().create_dataset_client(name='test_dataset')
    yield client
    await client.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""
    configuration = Configuration(purge_on_start=True)

    # Create dataset and add data
    dataset_client1 = await MemoryStorageClient().create_dataset_client(
        name='test_purge_dataset',
        configuration=configuration,
    )
    await dataset_client1.push_data({'item': 'initial data'})

    # Verify data was added
    items = await dataset_client1.get_data()
    assert len(items.items) == 1

    # Reopen with same storage client instance
    dataset_client2 = await MemoryStorageClient().create_dataset_client(
        name='test_purge_dataset',
        configuration=configuration,
    )

    # Verify data was purged (memory storage specific behavior)
    items = await dataset_client2.get_data()
    assert len(items.items) == 0


async def test_memory_metadata_updates(dataset_client: MemoryDatasetClient) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await dataset_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await dataset_client.get_data()

    # Verify timestamps (memory-specific behavior)
    metadata = await dataset_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await dataset_client.push_data({'new': 'item'})

    # Verify timestamps were updated
    metadata = await dataset_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read
