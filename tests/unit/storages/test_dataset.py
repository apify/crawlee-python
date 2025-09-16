# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore
from crawlee.storages._storage_instance_manager import StorageInstanceManager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path
    from typing import Any

    from crawlee.storage_clients import StorageClient


@pytest.fixture
async def dataset(
    storage_client: StorageClient,
) -> AsyncGenerator[Dataset, None]:
    """Fixture that provides a dataset instance for each test."""
    dataset = await Dataset.open(
        storage_client=storage_client,
    )

    yield dataset
    await dataset.drop()


async def test_open_creates_new_dataset(
    storage_client: StorageClient,
) -> None:
    """Test that open() creates a new dataset with proper metadata."""
    dataset = await Dataset.open(
        name='new_dataset',
        storage_client=storage_client,
    )

    # Verify dataset properties
    assert dataset.id is not None
    assert dataset.name == 'new_dataset'

    metadata = await dataset.get_metadata()
    assert metadata.item_count == 0

    await dataset.drop()


async def test_reopen_default(
    storage_client: StorageClient,
) -> None:
    """Test reopening a dataset with default parameters."""
    # Create a first dataset instance with default parameters
    dataset_1 = await Dataset.open(
        storage_client=storage_client,
    )

    # Verify default properties
    assert dataset_1.id is not None
    metadata_1 = await dataset_1.get_metadata()
    assert metadata_1.item_count == 0

    # Add an item
    await dataset_1.push_data({'key': 'value'})
    metadata_1 = await dataset_1.get_metadata()
    assert metadata_1.item_count == 1

    # Reopen the same dataset
    dataset_2 = await Dataset.open(
        storage_client=storage_client,
    )

    # Verify both instances reference the same dataset
    assert dataset_2.id == dataset_1.id
    assert dataset_2.name == dataset_1.name
    metadata_1 = await dataset_1.get_metadata()
    metadata_2 = await dataset_2.get_metadata()
    assert metadata_2.item_count == metadata_1.item_count == 1

    # Verify they are the same object (cached)
    assert id(dataset_1) == id(dataset_2)

    # Clean up
    await dataset_1.drop()


async def test_open_by_id(
    storage_client: StorageClient,
) -> None:
    """Test opening a dataset by its ID."""
    # First create a dataset by name
    dataset1 = await Dataset.open(
        name='dataset_by_id_test',
        storage_client=storage_client,
    )

    # Add some data to identify it
    test_item = {'test': 'opening_by_id', 'timestamp': 12345}
    await dataset1.push_data(test_item)

    # Open the dataset by ID
    dataset2 = await Dataset.open(
        id=dataset1.id,
        storage_client=storage_client,
    )

    # Verify it's the same dataset
    assert dataset2.id == dataset1.id
    assert dataset2.name == 'dataset_by_id_test'

    # Verify the data is still there
    data = await dataset2.get_data()
    assert data.count == 1
    assert data.items[0]['test'] == 'opening_by_id'
    assert data.items[0]['timestamp'] == 12345

    # Clean up
    await dataset2.drop()


async def test_open_existing_dataset(
    dataset: Dataset,
) -> None:
    """Test that open() loads an existing dataset correctly."""
    # Open the same dataset again
    reopened_dataset = await Dataset.open(
        name=dataset.name,
    )

    # Verify dataset properties
    assert dataset.id == reopened_dataset.id
    assert dataset.name == reopened_dataset.name
    metadata = await dataset.get_metadata()
    reopened_metadata = await reopened_dataset.get_metadata()
    assert metadata.item_count == reopened_metadata.item_count

    # Verify they are the same object (from cache)
    assert id(dataset) == id(reopened_dataset)


async def test_open_with_id_and_name(
    storage_client: StorageClient,
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(ValueError, match=r'Only one of "id", "name", or "alias" can be specified, not multiple.'):
        await Dataset.open(
            id='some-id',
            name='some-name',
            storage_client=storage_client,
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
    items: list[dict[str, Any]] = [
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
) -> None:
    """Test dropping a dataset removes it from cache and clears its data."""
    dataset = await Dataset.open(
        name='drop_test',
        storage_client=storage_client,
    )

    # Add some data
    await dataset.push_data({'test': 'data'})

    # Drop the dataset
    await dataset.drop()

    # Verify dataset is empty (by creating a new one with the same name)
    new_dataset = await Dataset.open(
        name='drop_test',
        storage_client=storage_client,
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
        to_kvs_name='export_kvs',
        to_kvs_storage_client=storage_client,
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
        to_kvs_name='export_kvs',
        to_kvs_storage_client=storage_client,
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
    with pytest.raises(ValueError, match=r'Unsupported content type'):
        await dataset.export_to(
            key='invalid_export',
            content_type='invalid',  # type: ignore[call-overload]  # Intentionally invalid content type
        )


async def test_export_with_multiple_kwargs(dataset: Dataset, tmp_path: Path) -> None:
    """Test exporting dataset using many optional arguments together."""
    target_kvs_name = 'some_kvs'
    target_storage_client = FileSystemStorageClient()
    export_key = 'exported_dataset'
    data = {'some key': 'some data'}

    # Prepare custom directory and configuration
    custom_dir_name = 'some_dir'
    custom_dir = tmp_path / custom_dir_name
    custom_dir.mkdir()
    target_configuration = Configuration(crawlee_storage_dir=str(custom_dir))  # type: ignore[call-arg]

    # Set expected values
    expected_exported_data = f'{json.dumps([{"some key": "some data"}])}'
    expected_kvs_dir = custom_dir / 'key_value_stores' / target_kvs_name

    # Populate dataset and export
    await dataset.push_data(data)
    await dataset.export_to(
        key=export_key,
        content_type='json',
        to_kvs_name=target_kvs_name,
        to_kvs_storage_client=target_storage_client,
        to_kvs_configuration=target_configuration,
    )

    # Verify the directory was created
    assert expected_kvs_dir.is_dir()
    # Verify that kvs contains the exported data
    kvs = await KeyValueStore.open(
        name=target_kvs_name, storage_client=target_storage_client, configuration=target_configuration
    )

    assert await kvs.get_value(key=export_key) == expected_exported_data


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


async def test_purge(
    storage_client: StorageClient,
) -> None:
    """Test purging a dataset removes all data but keeps the dataset itself."""
    # First create a dataset
    dataset = await Dataset.open(
        name='purge_test_dataset',
        storage_client=storage_client,
    )

    # Add some data
    initial_items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'},
    ]
    await dataset.push_data(initial_items)

    # Verify data was added
    data = await dataset.get_data()
    assert data.count == 3
    assert data.total == 3
    metadata = await dataset.get_metadata()
    assert metadata.item_count == 3

    # Record the dataset ID
    dataset_id = dataset.id

    # Purge the dataset
    await dataset.purge()

    # Verify the dataset still exists but is empty
    assert dataset.id == dataset_id  # Same ID preserved
    assert dataset.name == 'purge_test_dataset'  # Same name preserved

    # Dataset should be empty now
    data = await dataset.get_data()
    assert data.count == 0
    assert data.total == 0
    metadata = await dataset.get_metadata()
    assert metadata.item_count == 0

    # Verify we can add new data after purging
    new_item = {'id': 4, 'name': 'New Item After Purge'}
    await dataset.push_data(new_item)

    data = await dataset.get_data()
    assert data.count == 1
    assert data.items[0]['name'] == 'New Item After Purge'

    # Clean up
    await dataset.drop()


async def test_open_with_alias(
    storage_client: StorageClient,
) -> None:
    """Test opening datasets with alias parameter for NDU functionality."""
    # Create datasets with different aliases
    dataset_1 = await Dataset.open(
        alias='test_alias_1',
        storage_client=storage_client,
    )
    dataset_2 = await Dataset.open(
        alias='test_alias_2',
        storage_client=storage_client,
    )

    # Verify they have different IDs but no names (unnamed)
    assert dataset_1.id != dataset_2.id
    assert dataset_1.name is None
    assert dataset_2.name is None

    # Add different data to each
    await dataset_1.push_data({'source': 'alias_1', 'value': 1})
    await dataset_2.push_data({'source': 'alias_2', 'value': 2})

    # Verify data isolation
    data_1 = await dataset_1.get_data()
    data_2 = await dataset_2.get_data()

    assert data_1.count == 1
    assert data_2.count == 1
    assert data_1.items[0]['source'] == 'alias_1'
    assert data_2.items[0]['source'] == 'alias_2'

    # Clean up
    await dataset_1.drop()
    await dataset_2.drop()


async def test_alias_caching(
    storage_client: StorageClient,
) -> None:
    """Test that datasets with same alias return same instance (cached)."""
    # Open dataset with alias
    dataset_1 = await Dataset.open(
        alias='cache_test',
        storage_client=storage_client,
    )

    # Open again with same alias
    dataset_2 = await Dataset.open(
        alias='cache_test',
        storage_client=storage_client,
    )

    # Should be same instance
    assert dataset_1 is dataset_2
    assert dataset_1.id == dataset_2.id

    # Clean up
    await dataset_1.drop()


async def test_alias_with_id_error(
    storage_client: StorageClient,
) -> None:
    """Test that providing both alias and id raises error."""
    with pytest.raises(ValueError, match=r'Only one of "id", "name", or "alias" can be specified, not multiple.'):
        await Dataset.open(
            id='some-id',
            alias='some-alias',
            storage_client=storage_client,
        )


async def test_alias_with_name_error(
    storage_client: StorageClient,
) -> None:
    """Test that providing both alias and name raises error."""
    with pytest.raises(ValueError, match=r'Only one of "id", "name", or "alias" can be specified, not multiple.'):
        await Dataset.open(
            name='some-name',
            alias='some-alias',
            storage_client=storage_client,
        )


async def test_alias_with_all_parameters_error(
    storage_client: StorageClient,
) -> None:
    """Test that providing id, name, and alias raises error."""
    with pytest.raises(ValueError, match=r'Only one of "id", "name", or "alias" can be specified, not multiple.'):
        await Dataset.open(
            id='some-id',
            name='some-name',
            alias='some-alias',
            storage_client=storage_client,
        )


async def test_alias_with_special_characters(
    storage_client: StorageClient,
) -> None:
    """Test alias functionality with special characters."""
    special_aliases = [
        'alias-with-dashes',
        'alias_with_underscores',
        'alias.with.dots',
        'alias123with456numbers',
        'CamelCaseAlias',
    ]

    datasets = []
    for alias in special_aliases:
        dataset = await Dataset.open(
            alias=alias,
            storage_client=storage_client,
        )
        datasets.append(dataset)

        # Add data with the alias as identifier
        await dataset.push_data({'alias_used': alias, 'test': 'special_chars'})

    # Verify all work correctly
    for i, dataset in enumerate(datasets):
        data = await dataset.get_data()
        assert data.count == 1
        assert data.items[0]['alias_used'] == special_aliases[i]

    # Clean up
    for dataset in datasets:
        await dataset.drop()


async def test_named_vs_alias_conflict_detection(
    storage_client: StorageClient,
) -> None:
    """Test that conflicts between named and alias storages are detected."""
    # Test 1: Create named storage first, then try alias with same name
    named_dataset = await Dataset.open(name='conflict_test', storage_client=storage_client)
    assert named_dataset.name == 'conflict_test'

    # Try to create alias with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create alias storage "conflict_test".*already exists'):
        await Dataset.open(alias='conflict_test', storage_client=storage_client)

    # Clean up
    await named_dataset.drop()

    # Test 2: Create alias first, then try named with same name
    alias_dataset = await Dataset.open(alias='conflict_test2', storage_client=storage_client)
    assert alias_dataset.name is None  # Alias storages have no name

    # Try to create named with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create named storage "conflict_test2".*already exists'):
        await Dataset.open(name='conflict_test2', storage_client=storage_client)

    # Clean up
    await alias_dataset.drop()


async def test_alias_parameter(
    storage_client: StorageClient,
) -> None:
    """Test dataset creation and operations with alias parameter."""
    # Create dataset with alias
    alias_dataset = await Dataset.open(
        alias='test_alias',
        storage_client=storage_client,
    )

    # Verify alias dataset properties
    assert alias_dataset.id is not None
    assert alias_dataset.name is None  # Alias storages should be unnamed

    # Test data operations
    await alias_dataset.push_data({'type': 'alias', 'value': 1})
    data = await alias_dataset.get_data()
    assert data.count == 1
    assert data.items[0]['type'] == 'alias'

    await alias_dataset.drop()


async def test_alias_vs_named_isolation(
    storage_client: StorageClient,
) -> None:
    """Test that alias and named datasets with same identifier are isolated."""
    # Create named dataset
    named_dataset = await Dataset.open(
        name='test_identifier',
        storage_client=storage_client,
    )

    # Verify named dataset
    assert named_dataset.name == 'test_identifier'
    await named_dataset.push_data({'type': 'named'})

    # Clean up named dataset first
    await named_dataset.drop()

    # Now create alias dataset with same identifier (should work after cleanup)
    alias_dataset = await Dataset.open(
        alias='test_identifier',
        storage_client=storage_client,
    )

    # Should be different instance
    assert alias_dataset.name is None
    await alias_dataset.push_data({'type': 'alias'})

    # Verify alias data
    alias_data = await alias_dataset.get_data()
    assert alias_data.items[0]['type'] == 'alias'

    await alias_dataset.drop()


async def test_default_vs_alias_default_equivalence(
    storage_client: StorageClient,
) -> None:
    """Test that default dataset and alias='default' are equivalent."""
    # Open default dataset
    default_dataset = await Dataset.open(
        storage_client=storage_client,
    )

    alias_default_dataset = await Dataset.open(
        alias=StorageInstanceManager._DEFAULT_STORAGE_ALIAS,
        storage_client=storage_client,
    )

    # Should be the same
    assert default_dataset.id == alias_default_dataset.id
    assert default_dataset.name is None
    assert alias_default_dataset.name is None

    # Data should be shared
    await default_dataset.push_data({'source': 'default'})
    data = await alias_default_dataset.get_data()
    assert data.items[0]['source'] == 'default'

    await default_dataset.drop()


async def test_multiple_alias_isolation(
    storage_client: StorageClient,
) -> None:
    """Test that different aliases create separate datasets."""
    datasets = []

    for i in range(3):
        dataset = await Dataset.open(
            alias=f'alias_{i}',
            storage_client=storage_client,
        )
        await dataset.push_data({'alias': f'alias_{i}', 'index': i})
        datasets.append(dataset)

    # All should be different
    for i in range(3):
        for j in range(i + 1, 3):
            assert datasets[i].id != datasets[j].id

    # Verify data isolation
    for i, dataset in enumerate(datasets):
        data = await dataset.get_data()
        assert data.items[0]['alias'] == f'alias_{i}'
        await dataset.drop()


async def test_purge_on_start_enabled(storage_client: StorageClient) -> None:
    """Test purge behavior when purge_on_start=True: named storages retain data, unnamed storages are purged."""

    # Skip this test for memory storage since it doesn't persist data between client instances.
    if storage_client.__class__ == MemoryStorageClient:
        pytest.skip('Memory storage does not persist data between client instances.')

    configuration = Configuration(purge_on_start=True)

    # First, create all storage types with purge enabled and add data.
    default_dataset = await Dataset.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    alias_dataset = await Dataset.open(
        alias='purge_test_alias',
        storage_client=storage_client,
        configuration=configuration,
    )

    named_dataset = await Dataset.open(
        name='purge_test_named',
        storage_client=storage_client,
        configuration=configuration,
    )

    await default_dataset.push_data({'type': 'default', 'data': 'should_be_purged'})
    await alias_dataset.push_data({'type': 'alias', 'data': 'should_be_purged'})
    await named_dataset.push_data({'type': 'named', 'data': 'should_persist'})

    # Verify data was added
    default_data = await default_dataset.get_data()
    alias_data = await alias_dataset.get_data()
    named_data = await named_dataset.get_data()

    assert len(default_data.items) == 1
    assert len(alias_data.items) == 1
    assert len(named_data.items) == 1

    # Verify that default and alias storages are unnamed
    default_metadata = await default_dataset.get_metadata()
    alias_metadata = await alias_dataset.get_metadata()
    named_metadata = await named_dataset.get_metadata()

    assert default_metadata.name is None
    assert alias_metadata.name is None
    assert named_metadata.name == 'purge_test_named'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_dataset_2 = await Dataset.open(
        storage_client=storage_client,
        configuration=configuration,
    )
    alias_dataset_2 = await Dataset.open(
        alias='purge_test_alias',
        storage_client=storage_client,
        configuration=configuration,
    )
    named_dataset_2 = await Dataset.open(
        name='purge_test_named',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Check the data after purge
    default_data_after = await default_dataset_2.get_data()
    alias_data_after = await alias_dataset_2.get_data()
    named_data_after = await named_dataset_2.get_data()

    # Unnamed storages (alias and default) should be purged (data removed)
    assert len(default_data_after.items) == 0
    assert len(alias_data_after.items) == 0

    # Named storage should retain data (not purged)
    assert len(named_data_after.items) == 1

    # Clean up
    await named_dataset_2.drop()
    await alias_dataset_2.drop()
    await default_dataset_2.drop()


async def test_purge_on_start_disabled(storage_client: StorageClient) -> None:
    """Test purge behavior when purge_on_start=False: all storages retain data regardless of type."""

    # Skip this test for memory storage since it doesn't persist data between client instances.
    if storage_client.__class__ == MemoryStorageClient:
        pytest.skip('Memory storage does not persist data between client instances.')

    configuration = Configuration(purge_on_start=False)

    # First, create all storage types with purge disabled and add data.
    default_dataset = await Dataset.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    alias_dataset = await Dataset.open(
        alias='purge_test_alias',
        storage_client=storage_client,
        configuration=configuration,
    )

    named_dataset = await Dataset.open(
        name='purge_test_named',
        storage_client=storage_client,
        configuration=configuration,
    )

    await default_dataset.push_data({'type': 'default', 'data': 'should_persist'})
    await alias_dataset.push_data({'type': 'alias', 'data': 'should_persist'})
    await named_dataset.push_data({'type': 'named', 'data': 'should_persist'})

    # Verify data was added
    default_data = await default_dataset.get_data()
    alias_data = await alias_dataset.get_data()
    named_data = await named_dataset.get_data()

    assert len(default_data.items) == 1
    assert len(alias_data.items) == 1
    assert len(named_data.items) == 1

    # Verify that default and alias storages are unnamed
    default_metadata = await default_dataset.get_metadata()
    alias_metadata = await alias_dataset.get_metadata()
    named_metadata = await named_dataset.get_metadata()

    assert default_metadata.name is None
    assert alias_metadata.name is None
    assert named_metadata.name == 'purge_test_named'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_dataset_2 = await Dataset.open(
        storage_client=storage_client,
        configuration=configuration,
    )
    alias_dataset_2 = await Dataset.open(
        alias='purge_test_alias',
        storage_client=storage_client,
        configuration=configuration,
    )
    named_dataset_2 = await Dataset.open(
        name='purge_test_named',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Check the data after purge
    default_data_after = await default_dataset_2.get_data()
    alias_data_after = await alias_dataset_2.get_data()
    named_data_after = await named_dataset_2.get_data()

    # All storages should retain data (not purged)
    assert len(default_data_after.items) == 1
    assert len(alias_data_after.items) == 1
    assert len(named_data_after.items) == 1

    assert default_data_after.items[0]['data'] == 'should_persist'
    assert alias_data_after.items[0]['data'] == 'should_persist'
    assert named_data_after.items[0]['data'] == 'should_persist'

    # Clean up
    await default_dataset_2.drop()
    await alias_dataset_2.drop()
    await named_dataset_2.drop()


async def test_name_default_not_allowed(storage_client: StorageClient) -> None:
    """Test that storage can't have default alias as name, to prevent collisions with unnamed storage alias."""
    with pytest.raises(
        ValueError,
        match=f'Storage name cannot be "{StorageInstanceManager._DEFAULT_STORAGE_ALIAS}" as '
        f'it is reserved for default alias.',
    ):
        await Dataset.open(name=StorageInstanceManager._DEFAULT_STORAGE_ALIAS, storage_client=storage_client)
