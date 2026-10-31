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

    from crawlee.storage_backends._memory import MemoryDatasetBackend


def circular_dict() -> dict[str, Any]:
    value: dict[str, Any] = {}
    value['self'] = value
    return value


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


@pytest.mark.parametrize(
    'item',
    [
        pytest.param(circular_dict(), id='circular reference'),
        pytest.param({'nested': {(1, 2): 'tuple key'}}, id='tuple key'),
    ],
)
async def test_push_data_rejects_what_file_system_rejects(
    dataset_backend: MemoryDatasetBackend,
    tmp_path: Path,
    item: dict[str, Any],
) -> None:
    """An item the file-system backend can't serialize raises the same error and stores nothing."""
    configuration = Configuration(storage_dir=str(tmp_path))
    fs_backend = await FileSystemStorageBackend().create_dataset_backend(configuration=configuration)

    with pytest.raises((TypeError, ValueError)) as fs_error:
        await fs_backend.push_data(item)

    with pytest.raises(fs_error.type, match=re.escape(str(fs_error.value))):
        await dataset_backend.push_data([{'valid': True}, item])

    metadata = await dataset_backend.get_metadata()
    assert metadata.item_count == 0
    assert (await dataset_backend.get_data()).items == []


async def test_push_data_serializes_like_file_system(dataset_backend: MemoryDatasetBackend, tmp_path: Path) -> None:
    """Values JSON doesn't support are stored as the file-system backend reads them back."""
    item = {'created_at': datetime(2026, 1, 1, tzinfo=UTC), 'tags': {'single'}, 1: 'int key'}
    configuration = Configuration(storage_dir=str(tmp_path))
    fs_backend = await FileSystemStorageBackend().create_dataset_backend(configuration=configuration)

    await fs_backend.push_data(item)  # ty: ignore[invalid-argument-type]
    await dataset_backend.push_data(item)  # ty: ignore[invalid-argument-type]

    fs_items = (await fs_backend.get_data()).items
    assert (await dataset_backend.get_data()).items == fs_items
    assert fs_items == [{'created_at': '2026-01-01 00:00:00+00:00', 'tags': "{'single'}", '1': 'int key'}]
