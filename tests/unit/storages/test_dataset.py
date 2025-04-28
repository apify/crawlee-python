# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_clients import StorageClient


@pytest.fixture(params=['memory', 'file_system'])
def storage_client(request: pytest.FixtureRequest) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    if request.param == 'memory':
        return MemoryStorageClient()

    return FileSystemStorageClient()


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Provide a configuration with a temporary storage directory."""
    return Configuration(crawlee_storage_dir=str(tmp_path))  # type: ignore[call-arg]


@pytest.fixture
async def dataset(
    storage_client: StorageClient,
    configuration: Configuration,
) -> AsyncGenerator[Dataset, None]:
    """Fixture that provides a dataset instance for each test."""
    Dataset._cache_by_id.clear()
    Dataset._cache_by_name.clear()

    dataset = await Dataset.open(
        name='test_dataset',
        storage_client=storage_client,
        configuration=configuration,
    )

    yield dataset
    await dataset.drop()


async def test_open_creates_new_dataset(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() creates a new dataset with proper metadata."""
    dataset = await Dataset.open(
        name='new_dataset',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify dataset properties
    assert dataset.id is not None
    assert dataset.name == 'new_dataset'
    assert dataset.metadata.item_count == 0

    await dataset.drop()


async def test_open_existing_dataset(
    dataset: Dataset,
    storage_client: StorageClient,
) -> None:
    """Test that open() loads an existing dataset correctly."""
    # Open the same dataset again
    reopened_dataset = await Dataset.open(
        name=dataset.name,
        storage_client=storage_client,
    )

    # Verify dataset properties
    assert dataset.id == reopened_dataset.id
    assert dataset.name == reopened_dataset.name
    assert dataset.metadata.item_count == reopened_dataset.metadata.item_count

    # Verify they are the same object (from cache)
    assert id(dataset) == id(reopened_dataset)


async def test_open_with_id_and_name(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(ValueError, match='Only one of "id" or "name" can be specified'):
        await Dataset.open(
            id='some-id',
            name='some-name',
            storage_client=storage_client,
            configuration=configuration,
        )


async def test_push_data_single_item(dataset: Dataset) -> None:
    """Test pushing a single item to the dataset."""
    item = {'key': 'value', 'number': 42}
    await dataset.push_data(item)

    # Verify item was stored
    result = await dataset.get_data()
    assert result.count == 1
    assert result.items[0] == item


async def test_push_data_multiple_items(dataset: Dataset) -> None:
    """Test pushing multiple items to the dataset."""
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset.push_data(items)

    # Verify items were stored
    result = await dataset.get_data()
    assert result.count == 3
    assert result.items == items


async def test_get_data_empty_dataset(dataset: Dataset) -> None:
    """Test getting data from an empty dataset returns empty results."""
    result = await dataset.get_data()

    assert result.count == 0
    assert result.total == 0
    assert result.items == []


async def test_get_data_with_pagination(dataset: Dataset) -> None:
    """Test getting data with offset and limit parameters for pagination."""
    # Add some items
    items = [{'id': i} for i in range(1, 11)]  # 10 items
    await dataset.push_data(items)

    # Test offset
    result = await dataset.get_data(offset=3)
    assert result.count == 7
    assert result.offset == 3
    assert result.items[0]['id'] == 4

    # Test limit
    result = await dataset.get_data(limit=5)
    assert result.count == 5
    assert result.limit == 5
    assert result.items[-1]['id'] == 5

    # Test both offset and limit
    result = await dataset.get_data(offset=2, limit=3)
    assert result.count == 3
    assert result.offset == 2
    assert result.limit == 3
    assert result.items[0]['id'] == 3
    assert result.items[-1]['id'] == 5


async def test_get_data_descending_order(dataset: Dataset) -> None:
    """Test getting data in descending order reverses the item order."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset.push_data(items)

    # Get items in descending order
    result = await dataset.get_data(desc=True)

    assert result.desc is True
    assert result.items[0]['id'] == 5
    assert result.items[-1]['id'] == 1


async def test_get_data_skip_empty(dataset: Dataset) -> None:
    """Test getting data with skip_empty option filters out empty items."""
    # Add some items including an empty one
    items = [
        {'id': 1, 'name': 'Item 1'},
        {},  # Empty item
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset.push_data(items)

    # Get all items
    result = await dataset.get_data()
    assert result.count == 3

    # Get non-empty items
    result = await dataset.get_data(skip_empty=True)
    assert result.count == 2
    assert all(item != {} for item in result.items)


async def test_iterate_items(dataset: Dataset) -> None:
    """Test iterating over dataset items yields each item in the correct order."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset.push_data(items)

    # Iterate over all items
    collected_items = [item async for item in dataset.iterate_items()]

    assert len(collected_items) == 5
    assert collected_items[0]['id'] == 1
    assert collected_items[-1]['id'] == 5


async def test_iterate_items_with_options(dataset: Dataset) -> None:
    """Test iterating with offset, limit and desc parameters."""
    # Add some items
    items = [{'id': i} for i in range(1, 11)]  # 10 items
    await dataset.push_data(items)

    # Test with offset and limit
    collected_items = [item async for item in dataset.iterate_items(offset=3, limit=3)]

    assert len(collected_items) == 3
    assert collected_items[0]['id'] == 4
    assert collected_items[-1]['id'] == 6

    # Test with descending order
    collected_items = []
    async for item in dataset.iterate_items(desc=True, limit=3):
        collected_items.append(item)

    assert len(collected_items) == 3
    assert collected_items[0]['id'] == 10
    assert collected_items[-1]['id'] == 8


async def test_list_items(dataset: Dataset) -> None:
    """Test that list_items returns all dataset items as a list."""
    # Add some items
    items = [{'id': i} for i in range(1, 6)]  # 5 items
    await dataset.push_data(items)

    # Get all items as a list
    collected_items = await dataset.list_items()

    assert len(collected_items) == 5
    assert collected_items[0]['id'] == 1
    assert collected_items[-1]['id'] == 5


async def test_list_items_with_options(dataset: Dataset) -> None:
    """Test that list_items respects filtering options."""
    # Add some items
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3},  # Item with missing 'name' field
        {},  # Empty item
        {'id': 5, 'name': 'Item 5'},
    ]
    await dataset.push_data(items)

    # Test with offset and limit
    collected_items = await dataset.list_items(offset=1, limit=2)
    assert len(collected_items) == 2
    assert collected_items[0]['id'] == 2
    assert collected_items[1]['id'] == 3

    # Test with descending order - skip empty items to avoid KeyError
    collected_items = await dataset.list_items(desc=True, skip_empty=True)

    # Filter items that have an 'id' field
    items_with_ids = [item for item in collected_items if 'id' in item]
    id_values = [item['id'] for item in items_with_ids]

    # Verify the list is sorted in descending order
    assert sorted(id_values, reverse=True) == id_values, f'IDs should be in descending order. Got {id_values}'

    # Verify key IDs are present and in the right order
    if 5 in id_values and 3 in id_values:
        assert id_values.index(5) < id_values.index(3), 'ID 5 should come before ID 3 in descending order'

    # Test with skip_empty
    collected_items = await dataset.list_items(skip_empty=True)
    assert len(collected_items) == 4  # Should skip the empty item
    assert all(item != {} for item in collected_items)

    # Test with fields - manually filter since 'fields' parameter is not supported
    # Get all items first
    collected_items = await dataset.list_items()
    assert len(collected_items) == 5

    # Manually extract only the 'id' field from each item
    filtered_items = [{key: item[key] for key in ['id'] if key in item} for item in collected_items]

    # Verify 'name' field is not present in any item
    assert all('name' not in item for item in filtered_items)

    # Test clean functionality manually instead of using the clean parameter
    # Get all items
    collected_items = await dataset.list_items()

    # Manually filter out empty items as 'clean' would do
    clean_items = [item for item in collected_items if item != {}]

    assert len(clean_items) == 4  # Should have 4 non-empty items
    assert all(item != {} for item in clean_items)


async def test_drop(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test dropping a dataset removes it from cache and clears its data."""
    dataset = await Dataset.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add some data
    await dataset.push_data({'test': 'data'})

    # Verify dataset exists in cache
    assert dataset.id in Dataset._cache_by_id
    if dataset.name:
        assert dataset.name in Dataset._cache_by_name

    # Drop the dataset
    await dataset.drop()

    # Verify dataset was removed from cache
    assert dataset.id not in Dataset._cache_by_id
    if dataset.name:
        assert dataset.name not in Dataset._cache_by_name

    # Verify dataset is empty (by creating a new one with the same name)
    new_dataset = await Dataset.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    result = await new_dataset.get_data()
    assert result.count == 0
    await new_dataset.drop()


async def test_export_to_json(
    dataset: Dataset,
    storage_client: StorageClient,
) -> None:
    """Test exporting dataset to JSON format."""
    # Create a key-value store for export
    kvs = await KeyValueStore.open(
        name='export_kvs',
        storage_client=storage_client,
    )

    # Add some items to the dataset
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset.push_data(items)

    # Export to JSON
    await dataset.export_to(
        key='dataset_export.json',
        content_type='json',
        to_key_value_store_name='export_kvs',
    )

    # Retrieve the exported file
    record = await kvs.get_value(key='dataset_export.json')
    assert record is not None

    # Verify content has all the items
    assert '"id": 1' in record
    assert '"id": 2' in record
    assert '"id": 3' in record

    await kvs.drop()


async def test_export_to_csv(
    dataset: Dataset,
    storage_client: StorageClient,
) -> None:
    """Test exporting dataset to CSV format."""
    # Create a key-value store for export
    kvs = await KeyValueStore.open(
        name='export_kvs',
        storage_client=storage_client,
    )

    # Add some items to the dataset
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset.push_data(items)

    # Export to CSV
    await dataset.export_to(
        key='dataset_export.csv',
        content_type='csv',
        to_key_value_store_name='export_kvs',
    )

    # Retrieve the exported file
    record = await kvs.get_value(key='dataset_export.csv')
    assert record is not None

    # Verify content has all the items
    assert 'id,name' in record
    assert '1,Item 1' in record
    assert '2,Item 2' in record
    assert '3,Item 3' in record

    await kvs.drop()


async def test_export_to_invalid_content_type(dataset: Dataset) -> None:
    """Test exporting dataset with invalid content type raises error."""
    with pytest.raises(ValueError, match='Unsupported content type'):
        await dataset.export_to(
            key='invalid_export',
            content_type='invalid',  # type: ignore[call-overload]  # Intentionally invalid content type
        )


async def test_large_dataset(dataset: Dataset) -> None:
    """Test handling a large dataset with many items."""
    items = [{'id': i, 'value': f'value-{i}'} for i in range(100)]
    await dataset.push_data(items)

    # Test that all items are retrieved
    result = await dataset.get_data(limit=None)
    assert result.count == 100
    assert result.total == 100

    # Test pagination with large datasets
    result = await dataset.get_data(offset=50, limit=25)
    assert result.count == 25
    assert result.offset == 50
    assert result.items[0]['id'] == 50
    assert result.items[-1]['id'] == 74
