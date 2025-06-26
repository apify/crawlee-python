from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients._file_system import FileSystemDatasetClient


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )


@pytest.fixture
async def dataset_client(configuration: Configuration) -> AsyncGenerator[FileSystemDatasetClient, None]:
    """A fixture for a file system dataset client."""
    client = await FileSystemStorageClient().create_dataset_client(
        name='test_dataset',
        configuration=configuration,
    )
    yield client
    await client.drop()


async def test_file_and_directory_creation(configuration: Configuration) -> None:
    """Test that file system dataset creates proper files and directories."""
    client = await FileSystemStorageClient().create_dataset_client(
        name='new_dataset',
        configuration=configuration,
    )

    # Verify files were created
    assert client.path_to_dataset.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata file structure
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == client.metadata.id
        assert metadata['name'] == 'new_dataset'
        assert metadata['item_count'] == 0

    await client.drop()


async def test_file_persistence_and_content_verification(dataset_client: FileSystemDatasetClient) -> None:
    """Test that data is properly persisted to files with correct content."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    # Verify files are created on disk
    all_files = list(dataset_client.path_to_dataset.glob('*.json'))
    assert len(all_files) == 2  # 1 data file + 1 metadata file

    # Verify actual file content
    data_files = [item for item in all_files if item.name != METADATA_FILENAME]
    assert len(data_files) == 1

    with Path(data_files[0]).open() as f:
        saved_item = json.load(f)
        assert saved_item == item

    # Test multiple items file creation
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    all_files = list(dataset_client.path_to_dataset.glob('*.json'))
    assert len(all_files) == 5  # 4 data files + 1 metadata file

    data_files = [f for f in all_files if f.name != METADATA_FILENAME]
    assert len(data_files) == 4  # Original item + 3 new items


async def test_drop_removes_files_from_disk(dataset_client: FileSystemDatasetClient) -> None:
    """Test that dropping a dataset removes the entire dataset directory from disk."""
    await dataset_client.push_data({'test': 'data'})

    assert dataset_client.path_to_dataset.exists()

    # Drop the dataset
    await dataset_client.drop()

    assert not dataset_client.path_to_dataset.exists()


async def test_metadata_file_updates(dataset_client: FileSystemDatasetClient) -> None:
    """Test that metadata file is updated correctly after operations."""
    # Record initial timestamps
    initial_created = dataset_client.metadata.created_at
    initial_accessed = dataset_client.metadata.accessed_at
    initial_modified = dataset_client.metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await dataset_client.get_data()

    # Verify timestamps
    assert dataset_client.metadata.created_at == initial_created
    assert dataset_client.metadata.accessed_at > initial_accessed
    assert dataset_client.metadata.modified_at == initial_modified

    accessed_after_get = dataset_client.metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at
    await dataset_client.push_data({'new': 'item'})

    # Verify timestamps again
    assert dataset_client.metadata.created_at == initial_created
    assert dataset_client.metadata.modified_at > initial_modified
    assert dataset_client.metadata.accessed_at > accessed_after_get

    # Verify metadata file is updated on disk
    with dataset_client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['item_count'] == 1


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same dataset."""
    storage_client = FileSystemStorageClient()

    # Create dataset and add data
    original_client = await storage_client.create_dataset_client(
        name='persistence-test',
        configuration=configuration,
    )

    test_data = {'test_item': 'test_value', 'id': 123}
    await original_client.push_data(test_data)

    dataset_id = original_client.metadata.id

    # Reopen by ID and verify data persists
    reopened_client = await storage_client.create_dataset_client(
        id=dataset_id,
        configuration=configuration,
    )

    data = await reopened_client.get_data()
    assert len(data.items) == 1
    assert data.items[0] == test_data

    await reopened_client.drop()
