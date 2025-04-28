# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import KeyValueStore

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
async def kvs(
    storage_client: StorageClient,
    configuration: Configuration,
) -> AsyncGenerator[KeyValueStore, None]:
    """Fixture that provides a key-value store instance for each test."""
    KeyValueStore._cache_by_id.clear()
    KeyValueStore._cache_by_name.clear()

    kvs = await KeyValueStore.open(
        name='test_kvs',
        storage_client=storage_client,
        configuration=configuration,
    )

    yield kvs
    await kvs.drop()


async def test_open_creates_new_kvs(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() creates a new key-value store with proper metadata."""
    kvs = await KeyValueStore.open(
        name='new_kvs',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify key-value store properties
    assert kvs.id is not None
    assert kvs.name == 'new_kvs'

    await kvs.drop()


async def test_open_existing_kvs(
    kvs: KeyValueStore,
    storage_client: StorageClient,
) -> None:
    """Test that open() loads an existing key-value store correctly."""
    # Open the same key-value store again
    reopened_kvs = await KeyValueStore.open(
        name=kvs.name,
        storage_client=storage_client,
    )

    # Verify key-value store properties
    assert kvs.id == reopened_kvs.id
    assert kvs.name == reopened_kvs.name

    # Verify they are the same object (from cache)
    assert id(kvs) == id(reopened_kvs)


async def test_open_with_id_and_name(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(ValueError, match='Only one of "id" or "name" can be specified'):
        await KeyValueStore.open(
            id='some-id',
            name='some-name',
            storage_client=storage_client,
            configuration=configuration,
        )


async def test_set_get_value(kvs: KeyValueStore) -> None:
    """Test setting and getting a value from the key-value store."""
    # Set a value
    test_key = 'test-key'
    test_value = {'data': 'value', 'number': 42}
    await kvs.set_value(test_key, test_value)

    # Get the value
    result = await kvs.get_value(test_key)
    assert result == test_value


async def test_get_value_nonexistent(kvs: KeyValueStore) -> None:
    """Test getting a nonexistent value returns None."""
    result = await kvs.get_value('nonexistent-key')
    assert result is None


async def test_get_value_with_default(kvs: KeyValueStore) -> None:
    """Test getting a nonexistent value with a default value."""
    default_value = {'default': True}
    result = await kvs.get_value('nonexistent-key', default_value=default_value)
    assert result == default_value


async def test_set_value_with_content_type(kvs: KeyValueStore) -> None:
    """Test setting a value with a specific content type."""
    test_key = 'test-json'
    test_value = {'data': 'value', 'items': [1, 2, 3]}
    await kvs.set_value(test_key, test_value, content_type='application/json')

    # Verify the value is retrievable
    result = await kvs.get_value(test_key)
    assert result == test_value


async def test_delete_value(kvs: KeyValueStore) -> None:
    """Test deleting a value from the key-value store."""
    # Set a value first
    test_key = 'delete-me'
    test_value = 'value to delete'
    await kvs.set_value(test_key, test_value)

    # Verify value exists
    assert await kvs.get_value(test_key) == test_value

    # Delete the value
    await kvs.delete_value(test_key)

    # Verify value is gone
    assert await kvs.get_value(test_key) is None


async def test_list_keys_empty_kvs(kvs: KeyValueStore) -> None:
    """Test listing keys from an empty key-value store."""
    keys = await kvs.list_keys()
    assert len(keys) == 0


async def test_list_keys(kvs: KeyValueStore) -> None:
    """Test listing keys from a key-value store with items."""
    # Add some items
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')
    await kvs.set_value('key3', 'value3')

    # List keys
    keys = await kvs.list_keys()

    # Verify keys
    assert len(keys) == 3
    key_names = [k.key for k in keys]
    assert 'key1' in key_names
    assert 'key2' in key_names
    assert 'key3' in key_names


async def test_list_keys_with_limit(kvs: KeyValueStore) -> None:
    """Test listing keys with a limit parameter."""
    # Add some items
    for i in range(10):
        await kvs.set_value(f'key{i}', f'value{i}')

    # List with limit
    keys = await kvs.list_keys(limit=5)
    assert len(keys) == 5


async def test_list_keys_with_exclusive_start_key(kvs: KeyValueStore) -> None:
    """Test listing keys with an exclusive start key."""
    # Add some items in a known order
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')
    await kvs.set_value('key3', 'value3')
    await kvs.set_value('key4', 'value4')
    await kvs.set_value('key5', 'value5')

    # Get all keys first to determine their order
    all_keys = await kvs.list_keys()
    all_key_names = [k.key for k in all_keys]

    if len(all_key_names) >= 3:
        # Start from the second key
        start_key = all_key_names[1]
        keys = await kvs.list_keys(exclusive_start_key=start_key)

        # We should get all keys after the start key
        expected_count = len(all_key_names) - all_key_names.index(start_key) - 1
        assert len(keys) == expected_count

        # First key should be the one after start_key
        first_returned_key = keys[0].key
        assert first_returned_key != start_key
        assert all_key_names.index(first_returned_key) > all_key_names.index(start_key)


async def test_iterate_keys(kvs: KeyValueStore) -> None:
    """Test iterating over keys in the key-value store."""
    # Add some items
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')
    await kvs.set_value('key3', 'value3')

    collected_keys = [key async for key in kvs.iterate_keys()]

    # Verify iteration result
    assert len(collected_keys) == 3
    key_names = [k.key for k in collected_keys]
    assert 'key1' in key_names
    assert 'key2' in key_names
    assert 'key3' in key_names


async def test_iterate_keys_with_limit(kvs: KeyValueStore) -> None:
    """Test iterating over keys with a limit parameter."""
    # Add some items
    for i in range(10):
        await kvs.set_value(f'key{i}', f'value{i}')

    collected_keys = [key async for key in kvs.iterate_keys(limit=5)]

    # Verify iteration result
    assert len(collected_keys) == 5


async def test_drop(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test dropping a key-value store removes it from cache and clears its data."""
    kvs = await KeyValueStore.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add some data
    await kvs.set_value('test', 'data')

    # Verify key-value store exists in cache
    assert kvs.id in KeyValueStore._cache_by_id
    if kvs.name:
        assert kvs.name in KeyValueStore._cache_by_name

    # Drop the key-value store
    await kvs.drop()

    # Verify key-value store was removed from cache
    assert kvs.id not in KeyValueStore._cache_by_id
    if kvs.name:
        assert kvs.name not in KeyValueStore._cache_by_name

    # Verify key-value store is empty (by creating a new one with the same name)
    new_kvs = await KeyValueStore.open(
        name='drop_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Attempt to get a previously stored value
    result = await new_kvs.get_value('test')
    assert result is None
    await new_kvs.drop()


async def test_complex_data_types(kvs: KeyValueStore) -> None:
    """Test storing and retrieving complex data types."""
    # Test nested dictionaries
    nested_dict = {
        'level1': {
            'level2': {
                'level3': 'deep value',
                'numbers': [1, 2, 3],
            },
        },
        'array': [{'a': 1}, {'b': 2}],
    }
    await kvs.set_value('nested', nested_dict)
    result = await kvs.get_value('nested')
    assert result == nested_dict

    # Test lists
    test_list = [1, 'string', True, None, {'key': 'value'}]
    await kvs.set_value('list', test_list)
    result = await kvs.get_value('list')
    assert result == test_list


async def test_string_data(kvs: KeyValueStore) -> None:
    """Test storing and retrieving string data."""
    # Plain string
    await kvs.set_value('string', 'simple string')
    result = await kvs.get_value('string')
    assert result == 'simple string'

    # JSON string
    json_string = json.dumps({'key': 'value'})
    await kvs.set_value('json_string', json_string)
    result = await kvs.get_value('json_string')
    assert result == json_string
