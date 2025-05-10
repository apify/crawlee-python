from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient
from crawlee.storage_clients._file_system import FileSystemDatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )


@pytest.fixture
async def dataset_client(configuration: Configuration) -> AsyncGenerator[FileSystemDatasetClient, None]:
    """A fixture for a file system dataset client."""
    client = await FileSystemStorageClient().open_dataset_client(
        name='test_dataset',
        configuration=configuration,
    )
    yield client
    await client.drop()


async def test_open_creates_new_dataset(configuration: Configuration) -> None:
    """Test that open() creates a new dataset with proper metadata when it doesn't exist."""
    client = await FileSystemStorageClient().open_dataset_client(
        name='new_dataset',
        configuration=configuration,
    )

    # Verify correct client type and properties
    assert isinstance(client, FileSystemDatasetClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_dataset'
    assert client.metadata.item_count == 0
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)

    # Verify files were created
    assert client.path_to_dataset.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata content
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == client.metadata.id
        assert metadata['name'] == 'new_dataset'
        assert metadata['item_count'] == 0


async def test_dataset_client_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=True clears existing data in the dataset."""
    configuration.purge_on_start = True

    # Create dataset and add data
    dataset_client1 = await FileSystemStorageClient().open_dataset_client(
        configuration=configuration,
    )
    await dataset_client1.push_data({'item': 'initial data'})

    # Verify data was added
    items = await dataset_client1.get_data()
    assert len(items.items) == 1

    # Reopen
    dataset_client2 = await FileSystemStorageClient().open_dataset_client(
        configuration=configuration,
    )

    # Verify data was purged
    items = await dataset_client2.get_data()
    assert len(items.items) == 0


async def test_dataset_client_no_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=False keeps existing data in the dataset."""
    configuration.purge_on_start = False

    # Create dataset and add data
    dataset_client1 = await FileSystemStorageClient().open_dataset_client(
        name='test-no-purge-dataset',
        configuration=configuration,
    )
    await dataset_client1.push_data({'item': 'preserved data'})

    # Reopen
    dataset_client2 = await FileSystemStorageClient().open_dataset_client(
        name='test-no-purge-dataset',
        configuration=configuration,
    )

    # Verify data was preserved
    items = await dataset_client2.get_data()
    assert len(items.items) == 1
    assert items.items[0]['item'] == 'preserved data'


async def test_push_data_single_item(dataset_client: FileSystemDatasetClient) -> None:
    """Test pushing a single item to the dataset."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    # Verify item count was updated
    assert dataset_client.metadata.item_count == 1

    all_files = list(dataset_client.path_to_dataset.glob('*.json'))
    assert len(all_files) == 2  # 1 data file + 1 metadata file

    # Verify item was persisted
    data_files = [item for item in all_files if item.name != METADATA_FILENAME]
    assert len(data_files) == 1

    # Verify file content
    with Path(data_files[0]).open() as f:
        saved_item = json.load(f)
        assert saved_item == item


async def test_push_data_multiple_items(dataset_client: FileSystemDatasetClient) -> None:
    """Test pushing multiple items to the dataset."""
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    # Verify item count was updated
    assert dataset_client.metadata.item_count == 3

    all_files = list(dataset_client.path_to_dataset.glob('*.json'))
    assert len(all_files) == 4  # 3 data files + 1 metadata file

    # Verify items were saved to files
    data_files = [f for f in all_files if f.name != METADATA_FILENAME]
    assert len(data_files) == 3


async def test_get_data_empty_dataset(dataset_client: FileSystemDatasetClient) -> None:
    """Test getting data from an empty dataset returns empty list."""
    result = await dataset_client.get_data()

    assert isinstance(result, DatasetItemsListPage)
    assert result.count == 0
    assert result.total == 0
    assert result.items == []


async def test_get_data_with_items(dataset_client: FileSystemDatasetClient) -> None:
    """Test getting data from a dataset returns all items in order with correct properties."""
    # Add some items
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    # Get all items
    result = await dataset_client.get_data()

    assert result.count == 3
    assert result.total == 3
    assert len(result.items) == 3
    assert result.items[0]['id'] == 1
    assert result.items[1]['id'] == 2
    assert result.items[2]['id'] == 3


async def test_get_data_with_pagination(dataset_client: FileSystemDatasetClient) -> None:
    """Test getting data with offset and limit parameters for pagination implementation."""
    # Add some items
    items = [{'id': i} for i in range(1, 11)]  # 10 items
    await dataset_client.push_data(items)

    # Test offset
    result = await dataset_client.get_data(offset=3)
    assert result.count == 7
    assert result.offset == 3
    assert result.items[0]['id'] == 4

    # Test limit
    result = await dataset_client.get_data(limit=5)
    assert result.count == 5
    assert result.limit == 5
    assert result.items[-1]['id'] == 5

    # Test both offset and limit
    result = await dataset_client.get_data(offset=2, limit=3)
    assert result.count == 3
    assert result.offset == 2
    assert result.limit == 3
    assert result.items[0]['id'] == 3
    assert result.items[-1]['id'] == 5


async def test_get_data_descending_order(dataset_client: FileSystemDatasetClient) -> None:
    """Test getting data in descending order reverses the item order."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset_client.push_data(items)

    # Get items in descending order
    result = await dataset_client.get_data(desc=True)

    assert result.desc is True
    assert result.items[0]['id'] == 5
    assert result.items[-1]['id'] == 1


async def test_get_data_skip_empty(dataset_client: FileSystemDatasetClient) -> None:
    """Test getting data with skip_empty option filters out empty items when True."""
    # Add some items including an empty one
    items = [
        {'id': 1, 'name': 'Item 1'},
        {},  # Empty item
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset_client.push_data(items)

    # Get all items
    result = await dataset_client.get_data()
    assert result.count == 3

    # Get non-empty items
    result = await dataset_client.get_data(skip_empty=True)
    assert result.count == 2
    assert all(item != {} for item in result.items)


async def test_iterate(dataset_client: FileSystemDatasetClient) -> None:
    """Test iterating over dataset items yields each item in the original order."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset_client.push_data(items)

    # Iterate over all items
    collected_items = [item async for item in dataset_client.iterate_items()]

    assert len(collected_items) == 5
    assert collected_items[0]['id'] == 1
    assert collected_items[-1]['id'] == 5


async def test_iterate_with_options(dataset_client: FileSystemDatasetClient) -> None:
    """Test iterating with offset, limit and desc parameters works the same as with get_data()."""
    # Add some items
    items = [{'id': i} for i in range(1, 11)]  # 10 items
    await dataset_client.push_data(items)

    # Test with offset and limit
    collected_items = [item async for item in dataset_client.iterate_items(offset=3, limit=3)]

    assert len(collected_items) == 3
    assert collected_items[0]['id'] == 4
    assert collected_items[-1]['id'] == 6

    # Test with descending order
    collected_items = []
    async for item in dataset_client.iterate_items(desc=True, limit=3):
        collected_items.append(item)

    assert len(collected_items) == 3
    assert collected_items[0]['id'] == 10
    assert collected_items[-1]['id'] == 8


async def test_drop(dataset_client: FileSystemDatasetClient) -> None:
    """Test dropping a dataset removes the entire dataset directory from disk."""
    await dataset_client.push_data({'test': 'data'})

    assert dataset_client.path_to_dataset.exists()

    # Drop the dataset
    await dataset_client.drop()

    assert not dataset_client.path_to_dataset.exists()


async def test_metadata_updates(dataset_client: FileSystemDatasetClient) -> None:
    """Test that metadata timestamps are updated correctly after read and write operations."""
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
