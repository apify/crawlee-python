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
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )


@pytest.fixture
async def kvs(
    storage_client: StorageClient,
    configuration: Configuration,
) -> AsyncGenerator[KeyValueStore, None]:
    """Fixture that provides a key-value store instance for each test."""
    kvs = await KeyValueStore.open(
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


async def test_open_by_id(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test opening a key-value store by its ID."""
    # First create a key-value store by name
    kvs1 = await KeyValueStore.open(
        name='kvs_by_id_test',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add some data to identify it
    await kvs1.set_value('test_key', {'test': 'opening_by_id', 'timestamp': 12345})

    # Open the key-value store by ID
    kvs2 = await KeyValueStore.open(
        id=kvs1.id,
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify it's the same key-value store
    assert kvs2.id == kvs1.id
    assert kvs2.name == 'kvs_by_id_test'

    # Verify the data is still there
    value = await kvs2.get_value('test_key')
    assert value is not None
    assert value['test'] == 'opening_by_id'
    assert value['timestamp'] == 12345

    # Clean up
    await kvs2.drop()


async def test_set_get_value(kvs: KeyValueStore) -> None:
    """Test setting and getting a value from the key-value store."""
    # Set a value
    test_key = 'test-key'
    test_value = {'data': 'value', 'number': 42}
    await kvs.set_value(test_key, test_value)

    # Get the value
    result = await kvs.get_value(test_key)
    assert result == test_value


async def test_set_get_none(kvs: KeyValueStore) -> None:
    """Test setting and getting None as a value."""
    test_key = 'none-key'
    await kvs.set_value(test_key, None)
    result = await kvs.get_value(test_key)
    assert result is None


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

    # Drop the key-value store
    await kvs.drop()

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


async def test_reopen_default(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test reopening the default key-value store."""
    # Open the default key-value store
    kvs1 = await KeyValueStore.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    # Set a value
    await kvs1.set_value('test_key', 'test_value')

    # Open the default key-value store again
    kvs2 = await KeyValueStore.open(
        storage_client=storage_client,
        configuration=configuration,
    )

    # Verify they are the same store
    assert kvs1.id == kvs2.id
    assert kvs1.name == kvs2.name

    # Verify the value is accessible
    value1 = await kvs1.get_value('test_key')
    value2 = await kvs2.get_value('test_key')
    assert value1 == value2 == 'test_value'

    # Verify they are the same object
    assert id(kvs1) == id(kvs2)


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


async def test_key_with_special_characters(kvs: KeyValueStore) -> None:
    """Test storing and retrieving values with keys containing special characters."""
    # Key with spaces, slashes, and special characters
    special_key = 'key with spaces/and/slashes!@#$%^&*()'
    test_value = 'Special key value'

    # Store the value with the special key
    await kvs.set_value(key=special_key, value=test_value)

    # Retrieve the value and verify it matches
    result = await kvs.get_value(key=special_key)
    assert result is not None
    assert result == test_value

    # Make sure the key is properly listed
    keys = await kvs.list_keys()
    key_names = [k.key for k in keys]
    assert special_key in key_names

    # Test key deletion
    await kvs.delete_value(key=special_key)
    assert await kvs.get_value(key=special_key) is None


async def test_data_persistence_on_reopen(configuration: Configuration) -> None:
    """Test that data persists when reopening a KeyValueStore."""
    kvs1 = await KeyValueStore.open(configuration=configuration)

    await kvs1.set_value('key_123', 'value_123')

    result1 = await kvs1.get_value('key_123')
    assert result1 == 'value_123'

    kvs2 = await KeyValueStore.open(configuration=configuration)

    result2 = await kvs2.get_value('key_123')
    assert result2 == 'value_123'
    assert await kvs1.list_keys() == await kvs2.list_keys()

    await kvs2.set_value('key_456', 'value_456')

    result1 = await kvs1.get_value('key_456')
    assert result1 == 'value_456'


async def test_purge(
    storage_client: StorageClient,
    configuration: Configuration,
) -> None:
    """Test purging a key-value store removes all values but keeps the store itself."""
    # First create a key-value store
    kvs = await KeyValueStore.open(
        name='purge_test_kvs',
        storage_client=storage_client,
        configuration=configuration,
    )

    # Add some values
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')
    await kvs.set_value('key3', {'complex': 'value', 'number': 42})

    # Verify values were added
    keys = await kvs.list_keys()
    assert len(keys) == 3

    # Record the store ID
    kvs_id = kvs.id

    # Purge the key-value store
    await kvs.purge()

    # Verify the store still exists but is empty
    assert kvs.id == kvs_id  # Same ID preserved
    assert kvs.name == 'purge_test_kvs'  # Same name preserved

    # Store should be empty now
    keys = await kvs.list_keys()
    assert len(keys) == 0

    # Values should no longer be accessible
    assert await kvs.get_value('key1') is None
    assert await kvs.get_value('key2') is None
    assert await kvs.get_value('key3') is None

    # Verify we can add new values after purging
    await kvs.set_value('new_key', 'new value after purge')

    value = await kvs.get_value('new_key')
    assert value == 'new value after purge'

    # Clean up
    await kvs.drop()


async def test_record_exists_nonexistent(kvs: KeyValueStore) -> None:
    """Test that record_exists returns False for a nonexistent key."""
    result = await kvs.record_exists('nonexistent-key')
    assert result is False


async def test_record_exists_after_set(kvs: KeyValueStore) -> None:
    """Test that record_exists returns True after setting a value."""
    test_key = 'exists-key'
    test_value = {'data': 'test'}

    # Initially should not exist
    assert await kvs.record_exists(test_key) is False

    # Set the value
    await kvs.set_value(test_key, test_value)

    # Now should exist
    assert await kvs.record_exists(test_key) is True


async def test_record_exists_after_delete(kvs: KeyValueStore) -> None:
    """Test that record_exists returns False after deleting a value."""
    test_key = 'exists-then-delete-key'
    test_value = 'will be deleted'

    # Set a value
    await kvs.set_value(test_key, test_value)
    assert await kvs.record_exists(test_key) is True

    # Delete the value
    await kvs.delete_value(test_key)

    # Should no longer exist
    assert await kvs.record_exists(test_key) is False


async def test_record_exists_with_none_value(kvs: KeyValueStore) -> None:
    """Test that record_exists returns True even when value is None."""
    test_key = 'none-value-key'

    # Set None as value
    await kvs.set_value(test_key, None)

    # Should still exist even though value is None
    assert await kvs.record_exists(test_key) is True

    # Verify we can distinguish between None value and nonexistent key
    assert await kvs.get_value(test_key) is None
    assert await kvs.record_exists(test_key) is True
    assert await kvs.record_exists('truly-nonexistent') is False


async def test_record_exists_different_content_types(kvs: KeyValueStore) -> None:
    """Test record_exists with different content types."""
    test_cases = [
        ('json-key', {'data': 'json'}, 'application/json'),
        ('text-key', 'plain text', 'text/plain'),
        ('binary-key', b'binary data', 'application/octet-stream'),
    ]

    for key, value, content_type in test_cases:
        # Set value with specific content type
        await kvs.set_value(key, value, content_type=content_type)

        # Should exist regardless of content type
        assert await kvs.record_exists(key) is True


async def test_record_exists_multiple_keys(kvs: KeyValueStore) -> None:
    """Test record_exists with multiple keys and batch operations."""
    keys_and_values = [
        ('key1', 'value1'),
        ('key2', {'nested': 'object'}),
        ('key3', [1, 2, 3]),
        ('key4', None),
    ]

    # Initially, none should exist
    for key, _ in keys_and_values:
        assert await kvs.record_exists(key) is False

    # Set all values
    for key, value in keys_and_values:
        await kvs.set_value(key, value)

    # All should exist now
    for key, _ in keys_and_values:
        assert await kvs.record_exists(key) is True

    # Test some non-existent keys
    assert await kvs.record_exists('nonexistent1') is False
    assert await kvs.record_exists('nonexistent2') is False


async def test_record_exists_after_purge(kvs: KeyValueStore) -> None:
    """Test that record_exists returns False after purging the store."""
    # Set some values
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')

    # Verify they exist
    assert await kvs.record_exists('key1') is True
    assert await kvs.record_exists('key2') is True

    # Purge the store
    await kvs.purge()

    # Should no longer exist
    assert await kvs.record_exists('key1') is False
    assert await kvs.record_exists('key2') is False
