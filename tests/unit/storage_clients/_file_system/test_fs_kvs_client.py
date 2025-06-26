from __future__ import annotations

import asyncio
import json
import urllib.parse
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient
from crawlee.storage_clients._file_system import FileSystemKeyValueStoreClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )


@pytest.fixture
async def kvs_client(configuration: Configuration) -> AsyncGenerator[FileSystemKeyValueStoreClient, None]:
    """A fixture for a file system key-value store client."""
    client = await FileSystemStorageClient().create_kvs_client(
        name='test_kvs',
        configuration=configuration,
    )
    yield client
    await client.drop()


async def test_open_creates_new_kvs(configuration: Configuration) -> None:
    """Test that open() creates a new key-value store with proper metadata and files on disk."""
    client = await FileSystemStorageClient().create_kvs_client(
        name='new_kvs',
        configuration=configuration,
    )

    # Verify correct client type and properties
    assert isinstance(client, FileSystemKeyValueStoreClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_kvs'
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)

    # Verify files were created
    assert client.path_to_kvs.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata content
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == client.metadata.id
        assert metadata['name'] == 'new_kvs'


async def test_open_kvs_by_id(configuration: Configuration) -> None:
    """Test opening a key-value store by ID after creating it by name."""
    storage_client = FileSystemStorageClient()

    # First create a key-value store by name
    original_client = await storage_client.create_kvs_client(
        name='open-by-id-test',
        configuration=configuration,
    )

    # Get the ID from the created client
    kvs_id = original_client.metadata.id

    # Add some data to verify it persists
    await original_client.set_value(key='test-key', value='test-value')

    # Now try to open the same key-value store using just the ID
    reopened_client = await storage_client.create_kvs_client(
        id=kvs_id,
        configuration=configuration,
    )

    # Verify it's the same key-value store
    assert reopened_client.metadata.id == kvs_id
    assert reopened_client.metadata.name == 'open-by-id-test'

    # Verify the data is still there
    record = await reopened_client.get_value(key='test-key')
    assert record is not None
    assert record.value == 'test-value'

    # Clean up
    await reopened_client.drop()


async def test_kvs_client_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=True clears existing data in the key-value store."""
    configuration.purge_on_start = True

    # Create KVS and add data
    kvs_client1 = await FileSystemStorageClient().create_kvs_client(
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen
    kvs_client2 = await FileSystemStorageClient().create_kvs_client(
        configuration=configuration,
    )

    # Verify value was purged
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_kvs_client_no_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=False keeps existing data in the key-value store."""
    configuration.purge_on_start = False

    # Create KVS and add data
    kvs_client1 = await FileSystemStorageClient().create_kvs_client(
        name='test-no-purge-kvs',
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='preserved value')

    # Reopen
    kvs_client2 = await FileSystemStorageClient().create_kvs_client(
        name='test-no-purge-kvs',
        configuration=configuration,
    )

    # Verify value was preserved
    record = await kvs_client2.get_value(key='test-key')
    assert record is not None
    assert record.value == 'preserved value'


async def test_set_get_value_string(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test setting and getting a string value with correct file creation and metadata."""
    # Set a value
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Check if the file was created
    key_path = kvs_client.path_to_kvs / test_key
    key_metadata_path = kvs_client.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert key_metadata_path.exists()

    # Check file content
    content = key_path.read_text(encoding='utf-8')
    assert content == test_value

    # Check record metadata
    with key_metadata_path.open() as f:
        metadata = json.load(f)
        assert metadata['key'] == test_key
        assert metadata['content_type'] == 'text/plain; charset=utf-8'
        assert metadata['size'] == len(test_value.encode('utf-8'))

    # Get the value
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.key == test_key
    assert record.value == test_value
    assert record.content_type == 'text/plain; charset=utf-8'
    assert record.size == len(test_value.encode('utf-8'))


async def test_set_get_value_json(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test setting and getting a JSON value with correct serialization and deserialization."""
    # Set a value
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_client.set_value(key=test_key, value=test_value)

    # Get the value
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.key == test_key
    assert record.value == test_value
    assert 'application/json' in record.content_type


async def test_set_get_value_bytes(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test setting and getting binary data without corruption and with correct content type."""
    # Set a value
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Get the value
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.key == test_key
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'
    assert record.size == len(test_value)


async def test_set_value_explicit_content_type(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that an explicitly provided content type overrides the automatically inferred one."""
    test_key = 'test-explicit-content-type'
    test_value = 'Hello, world!'
    explicit_content_type = 'text/html; charset=utf-8'

    await kvs_client.set_value(key=test_key, value=test_value, content_type=explicit_content_type)

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.content_type == explicit_content_type


async def test_get_nonexistent_value(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that attempting to get a non-existent key returns None."""
    record = await kvs_client.get_value(key='nonexistent-key')
    assert record is None


async def test_overwrite_value(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that an existing value can be overwritten and the updated value is retrieved correctly."""
    test_key = 'test-overwrite'

    # Set initial value
    initial_value = 'Initial value'
    await kvs_client.set_value(key=test_key, value=initial_value)

    # Overwrite with new value
    new_value = 'New value'
    await kvs_client.set_value(key=test_key, value=new_value)

    # Verify the updated value
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == new_value


async def test_delete_value(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that deleting a value removes its files from disk and makes it irretrievable."""
    test_key = 'test-delete'
    test_value = 'Delete me'

    # Set a value
    await kvs_client.set_value(key=test_key, value=test_value)

    # Verify it exists
    key_path = kvs_client.path_to_kvs / test_key
    metadata_path = kvs_client.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert metadata_path.exists()

    # Delete the value
    await kvs_client.delete_value(key=test_key)

    # Verify files were deleted
    assert not key_path.exists()
    assert not metadata_path.exists()

    # Verify value is no longer retrievable
    record = await kvs_client.get_value(key=test_key)
    assert record is None


async def test_delete_nonexistent_value(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that attempting to delete a non-existent key is a no-op and doesn't raise errors."""
    # Should not raise an error
    await kvs_client.delete_value(key='nonexistent-key')


async def test_iterate_keys_empty_store(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that iterating over an empty store yields no keys."""
    keys = [key async for key in kvs_client.iterate_keys()]
    assert len(keys) == 0


async def test_iterate_keys(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that all keys can be iterated over and are returned in sorted order."""
    # Add some values
    await kvs_client.set_value(key='key1', value='value1')
    await kvs_client.set_value(key='key2', value='value2')
    await kvs_client.set_value(key='key3', value='value3')

    # Iterate over keys
    keys = [key.key async for key in kvs_client.iterate_keys()]
    assert len(keys) == 3
    assert sorted(keys) == ['key1', 'key2', 'key3']


async def test_iterate_keys_with_limit(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that the limit parameter returns only the specified number of keys."""
    # Add some values
    await kvs_client.set_value(key='key1', value='value1')
    await kvs_client.set_value(key='key2', value='value2')
    await kvs_client.set_value(key='key3', value='value3')

    # Iterate with limit
    keys = [key.key async for key in kvs_client.iterate_keys(limit=2)]
    assert len(keys) == 2


async def test_iterate_keys_with_exclusive_start_key(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that exclusive_start_key parameter returns only keys after it alphabetically."""
    # Add some values with alphabetical keys
    await kvs_client.set_value(key='a-key', value='value-a')
    await kvs_client.set_value(key='d-key', value='value-d')
    await kvs_client.set_value(key='c-key', value='value-c')
    await kvs_client.set_value(key='b-key', value='value-b')

    # Iterate with exclusive start key
    keys = [key.key async for key in kvs_client.iterate_keys(exclusive_start_key='b-key')]
    assert len(keys) == 2
    assert 'c-key' in keys
    assert 'd-key' in keys
    assert 'a-key' not in keys
    assert 'b-key' not in keys


async def test_drop(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that drop removes the entire store directory from disk."""
    await kvs_client.set_value(key='test', value='test-value')

    assert kvs_client.path_to_kvs.exists()

    # Drop the store
    await kvs_client.drop()

    assert not kvs_client.path_to_kvs.exists()


async def test_metadata_updates(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that read/write operations properly update accessed_at and modified_at timestamps."""
    # Record initial timestamps
    initial_created = kvs_client.metadata.created_at
    initial_accessed = kvs_client.metadata.accessed_at
    initial_modified = kvs_client.metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await kvs_client.get_value(key='nonexistent')

    # Verify timestamps
    assert kvs_client.metadata.created_at == initial_created
    assert kvs_client.metadata.accessed_at > initial_accessed
    assert kvs_client.metadata.modified_at == initial_modified

    accessed_after_get = kvs_client.metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at
    await kvs_client.set_value(key='new-key', value='new-value')

    # Verify timestamps again
    assert kvs_client.metadata.created_at == initial_created
    assert kvs_client.metadata.modified_at > initial_modified
    assert kvs_client.metadata.accessed_at > accessed_after_get


async def test_get_public_url(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that get_public_url returns a valid file:// URL for the given key."""
    # Set a value first to ensure the file exists
    test_key = 'test-url-key'
    test_value = 'Test URL value'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Get the URL
    url = await kvs_client.get_public_url(key=test_key)

    # Verify it's a valid file:// URL
    assert url.startswith('file:///')

    # The encoded key name should be in the URL
    encoded_key = urllib.parse.quote(test_key, safe='')
    assert encoded_key in url

    # Verify the path in the URL points to the actual file
    file_path = kvs_client.path_to_kvs / encoded_key
    assert file_path.exists()

    # Verify file content without using urlopen (avoiding blocking IO)
    content = file_path.read_text(encoding='utf-8')
    assert content == test_value


async def test_concurrent_operations(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that multiple concurrent set operations can be performed safely with correct results."""

    # Create multiple tasks to set different values concurrently
    async def set_value(key: str, value: str) -> None:
        await kvs_client.set_value(key=key, value=value)

    tasks = [asyncio.create_task(set_value(f'concurrent-key-{i}', f'value-{i}')) for i in range(10)]

    # Wait for all tasks to complete
    await asyncio.gather(*tasks)

    # Verify all values were set correctly
    for i in range(10):
        key = f'concurrent-key-{i}'
        record = await kvs_client.get_value(key=key)
        assert record is not None
        assert record.value == f'value-{i}'


async def test_record_exists_nonexistent_key(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that record_exists returns False for nonexistent key."""
    assert await kvs_client.record_exists(key='nonexistent-key') is False


async def test_record_exists_after_set_dict(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test record_exists returns True after setting a dict value."""
    key = 'dict-key'
    value = {'data': 'test'}

    # Initially should not exist
    assert await kvs_client.record_exists(key=key) is False

    # Set the value and check existence
    await kvs_client.set_value(key=key, value=value)
    assert await kvs_client.record_exists(key=key) is True

    # Also verify we can retrieve the value
    record = await kvs_client.get_value(key=key)
    assert record is not None
    assert record.value == value

    # Verify the actual files exist on disk
    encoded_key = urllib.parse.quote(key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
    assert record_path.exists()
    assert metadata_path.exists()


async def test_record_exists_after_set_string(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test record_exists returns True after setting a string value."""
    key = 'string-key'
    value = 'test string'

    # Initially should not exist
    assert await kvs_client.record_exists(key=key) is False

    # Set the value and check existence
    await kvs_client.set_value(key=key, value=value)
    assert await kvs_client.record_exists(key=key) is True

    # Also verify we can retrieve the value
    record = await kvs_client.get_value(key=key)
    assert record is not None
    assert record.value == value

    # Verify the actual files exist on disk
    encoded_key = urllib.parse.quote(key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
    assert record_path.exists()
    assert metadata_path.exists()


async def test_record_exists_after_set_none(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test record_exists returns True after setting None value."""
    key = 'none-key'
    value = None

    # Initially should not exist
    assert await kvs_client.record_exists(key=key) is False

    # Set the value and check existence
    await kvs_client.set_value(key=key, value=value)
    assert await kvs_client.record_exists(key=key) is True

    # Also verify we can retrieve the value
    record = await kvs_client.get_value(key=key)
    assert record is not None
    assert record.value == value

    # Verify the actual files exist on disk
    encoded_key = urllib.parse.quote(key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
    assert record_path.exists()
    assert metadata_path.exists()


async def test_record_exists_after_set_int(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test record_exists returns True after setting an int value."""
    key = 'int-key'
    value = 42

    # Initially should not exist
    assert await kvs_client.record_exists(key=key) is False

    # Set the value and check existence
    await kvs_client.set_value(key=key, value=value)
    assert await kvs_client.record_exists(key=key) is True

    # Also verify we can retrieve the value
    record = await kvs_client.get_value(key=key)
    assert record is not None
    # For file system storage, non-JSON scalar values get converted to strings
    assert record.value == str(value)

    # Verify the actual files exist on disk
    encoded_key = urllib.parse.quote(key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
    assert record_path.exists()
    assert metadata_path.exists()


async def test_record_exists_after_delete(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test record_exists returns False after deleting a value."""
    key = 'delete-key'
    value = 'will be deleted'

    # Initially should not exist
    assert await kvs_client.record_exists(key=key) is False

    # Set the value first
    await kvs_client.set_value(key=key, value=value)
    assert await kvs_client.record_exists(key=key) is True

    # Then delete it
    await kvs_client.delete_value(key=key)
    assert await kvs_client.record_exists(key=key) is False

    # Verify the actual files are gone from disk
    encoded_key = urllib.parse.quote(key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
    assert not record_path.exists()
    assert not metadata_path.exists()


async def test_record_exists_none_value_distinction(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that record_exists can distinguish between None value and nonexistent key."""
    test_key = 'none-value-key'

    # Set None as value
    await kvs_client.set_value(key=test_key, value=None)

    # Should still exist even though value is None
    assert await kvs_client.record_exists(key=test_key) is True

    # Verify we can distinguish between None value and nonexistent key
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value is None
    assert await kvs_client.record_exists(key=test_key) is True
    assert await kvs_client.record_exists(key='truly-nonexistent') is False


async def test_record_exists_only_value_file(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that record_exists returns False if only value file exists without metadata."""
    test_key = 'only-value-file-key'

    # Manually create only the value file without metadata
    encoded_key = urllib.parse.quote(test_key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text('orphaned value')

    # Should return False because metadata file is missing
    assert await kvs_client.record_exists(key=test_key) is False


async def test_record_exists_only_metadata_file(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that record_exists returns False if only metadata file exists without value."""
    test_key = 'only-metadata-file-key'

    # Manually create only the metadata file without value
    encoded_key = urllib.parse.quote(test_key, safe='')
    record_path = kvs_client.path_to_kvs / encoded_key
    metadata_path = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')

    record_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text('{"key":"test","content_type":"text/plain","size":10}')

    # Should return False because value file is missing
    assert await kvs_client.record_exists(key=test_key) is False


async def test_record_exists_updates_metadata(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that record_exists updates the accessed_at timestamp."""
    # Record initial timestamp
    initial_accessed = kvs_client.metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Check if record exists (should update accessed_at)
    await kvs_client.record_exists(key='any-key')

    # Verify timestamp was updated
    assert kvs_client.metadata.accessed_at > initial_accessed
