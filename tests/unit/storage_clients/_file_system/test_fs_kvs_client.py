from __future__ import annotations

import asyncio
import json
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
    client = await FileSystemStorageClient().open_key_value_store_client(
        name='test_kvs',
        configuration=configuration,
    )
    yield client
    await client.drop()


async def test_open_creates_new_kvs(configuration: Configuration) -> None:
    """Test that open() creates a new key-value store with proper metadata and files on disk."""
    client = await FileSystemStorageClient().open_key_value_store_client(
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


async def test_kvs_client_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=True clears existing data in the key-value store."""
    configuration.purge_on_start = True

    # Create KVS and add data
    kvs_client1 = await FileSystemStorageClient().open_key_value_store_client(
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen
    kvs_client2 = await FileSystemStorageClient().open_key_value_store_client(
        configuration=configuration,
    )

    # Verify value was purged
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_kvs_client_no_purge_on_start(configuration: Configuration) -> None:
    """Test that purge_on_start=False keeps existing data in the key-value store."""
    configuration.purge_on_start = False

    # Create KVS and add data
    kvs_client1 = await FileSystemStorageClient().open_key_value_store_client(
        name='test-no-purge-kvs',
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='preserved value')

    # Reopen
    kvs_client2 = await FileSystemStorageClient().open_key_value_store_client(
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
    await kvs_client.set_value(key='b-key', value='value-b')
    await kvs_client.set_value(key='c-key', value='value-c')
    await kvs_client.set_value(key='d-key', value='value-d')

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


async def test_get_public_url_not_supported(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that get_public_url raises NotImplementedError for the file system implementation."""
    with pytest.raises(NotImplementedError, match='Public URLs are not supported'):
        await kvs_client.get_public_url(key='any-key')


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
