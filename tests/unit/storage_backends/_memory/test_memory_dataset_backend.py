from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_backends import MemoryStorageBackend

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_backends._memory import MemoryDatasetBackend


@pytest.fixture
async def dataset_backend() -> AsyncGenerator[MemoryDatasetBackend, None]:
    """Fixture that provides a fresh memory dataset backend for each test."""
    backend = await MemoryStorageBackend().create_dataset_backend(name='test-dataset')
    yield backend
    await backend.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""
    # Create dataset and add data
    dataset_backend1 = await MemoryStorageBackend().create_dataset_backend(
        name='test-purge-dataset',
    )
    await dataset_backend1.push_data({'item': 'initial data'})

    # Verify data was added
    items = await dataset_backend1.get_data()
    assert len(items.items) == 1

    # Reopen with same storage backend instance
    dataset_backend2 = await MemoryStorageBackend().create_dataset_backend(
        name='test-purge-dataset',
    )

    # Verify data was purged (memory storage specific behavior)
    items = await dataset_backend2.get_data()
    assert len(items.items) == 0


async def test_memory_metadata_updates(dataset_backend: MemoryDatasetBackend) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await dataset_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await dataset_backend.get_data()

    # Verify timestamps (memory-specific behavior)
    metadata = await dataset_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await dataset_backend.push_data({'new': 'item'})

    # Verify timestamps were updated
    metadata = await dataset_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read
