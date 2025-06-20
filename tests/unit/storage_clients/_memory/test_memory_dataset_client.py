from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storage_clients._memory import MemoryDatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def dataset_client() -> AsyncGenerator[MemoryDatasetClient, None]:
    """Fixture that provides a fresh memory dataset client for each test."""
    client = await MemoryStorageClient().create_dataset_client(name='test_dataset')
    yield client
    await client.drop()


async def test_open_creates_new_dataset() -> None:
    """Test that open() creates a new dataset with proper metadata and adds it to the cache."""
    client = await MemoryStorageClient().create_dataset_client(name='new_dataset')

    # Verify correct client type and properties
    assert isinstance(client, MemoryDatasetClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_dataset'
    assert client.metadata.item_count == 0
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)


async def test_dataset_client_purge_on_start() -> None:
    """Test that purge_on_start=True clears existing data in the dataset."""
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

    # Reopen
    dataset_client2 = await MemoryStorageClient().create_dataset_client(
        name='test_purge_dataset',
        configuration=configuration,
    )

    # Verify data was purged
    items = await dataset_client2.get_data()
    assert len(items.items) == 0


async def test_open_with_id_and_name() -> None:
    """Test that open() can be used with both id and name parameters."""
    client = await MemoryStorageClient().create_dataset_client(
        id='some-id',
        name='some-name',
    )
    assert client.metadata.id == 'some-id'
    assert client.metadata.name == 'some-name'


async def test_push_data_single_item(dataset_client: MemoryDatasetClient) -> None:
    """Test pushing a single item to the dataset and verifying it was stored correctly."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    # Verify item count was updated
    assert dataset_client.metadata.item_count == 1

    # Verify item was stored
    result = await dataset_client.get_data()
    assert result.count == 1
    assert result.items[0] == item


async def test_push_data_multiple_items(dataset_client: MemoryDatasetClient) -> None:
    """Test pushing multiple items to the dataset and verifying they were stored correctly."""
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset_client.push_data(items)

    # Verify item count was updated
    assert dataset_client.metadata.item_count == 3

    # Verify items were stored
    result = await dataset_client.get_data()
    assert result.count == 3
    assert result.items == items


async def test_get_data_empty_dataset(dataset_client: MemoryDatasetClient) -> None:
    """Test that getting data from an empty dataset returns empty results with correct metadata."""
    result = await dataset_client.get_data()

    assert isinstance(result, DatasetItemsListPage)
    assert result.count == 0
    assert result.total == 0
    assert result.items == []


async def test_get_data_with_items(dataset_client: MemoryDatasetClient) -> None:
    """Test that all items pushed to the dataset can be retrieved with correct metadata."""
    # Add some items
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset_client.push_data(items)

    # Get all items
    result = await dataset_client.get_data()

    assert result.count == 3
    assert result.total == 3
    assert len(result.items) == 3
    assert result.items[0]['id'] == 1
    assert result.items[1]['id'] == 2
    assert result.items[2]['id'] == 3


async def test_get_data_with_pagination(dataset_client: MemoryDatasetClient) -> None:
    """Test that offset and limit parameters work correctly for dataset pagination."""
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


async def test_get_data_descending_order(dataset_client: MemoryDatasetClient) -> None:
    """Test that the desc parameter correctly reverses the order of returned items."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset_client.push_data(items)

    # Get items in descending order
    result = await dataset_client.get_data(desc=True)

    assert result.desc is True
    assert result.items[0]['id'] == 5
    assert result.items[-1]['id'] == 1


async def test_get_data_skip_empty(dataset_client: MemoryDatasetClient) -> None:
    """Test that the skip_empty parameter correctly filters out empty items."""
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


async def test_iterate(dataset_client: MemoryDatasetClient) -> None:
    """Test that iterate_items yields each item in the dataset in the correct order."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset_client.push_data(items)

    # Iterate over all items
    collected_items = [item async for item in dataset_client.iterate_items()]

    assert len(collected_items) == 5
    assert collected_items[0]['id'] == 1
    assert collected_items[-1]['id'] == 5


async def test_iterate_with_options(dataset_client: MemoryDatasetClient) -> None:
    """Test that iterate_items respects offset, limit, and desc parameters."""
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


async def test_drop(dataset_client: MemoryDatasetClient) -> None:
    """Test that drop removes the dataset from cache and resets its state."""
    await dataset_client.push_data({'test': 'data'})

    # Drop the dataset
    await dataset_client.drop()

    # Verify the dataset is empty
    assert dataset_client.metadata.item_count == 0
    result = await dataset_client.get_data()
    assert result.count == 0


async def test_metadata_updates(dataset_client: MemoryDatasetClient) -> None:
    """Test that read/write operations properly update accessed_at and modified_at timestamps."""
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
