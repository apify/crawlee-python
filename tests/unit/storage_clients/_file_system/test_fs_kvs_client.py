from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_clients._file_system import FileSystemKeyValueStoreClient


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


async def test_file_and_directory_creation(configuration: Configuration) -> None:
    """Test that file system KVS creates proper files and directories."""
    client = await FileSystemStorageClient().create_kvs_client(
        name='new_kvs',
        configuration=configuration,
    )

    # Verify files were created
    assert client.path_to_kvs.exists()
    assert client.path_to_metadata.exists()

    # Verify metadata file structure
    with client.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == (await client.get_metadata()).id
        assert metadata['name'] == 'new_kvs'

    await client.drop()


async def test_value_file_creation_and_content(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that values are properly persisted to files with correct content and metadata."""
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Check if the files were created
    key_path = kvs_client.path_to_kvs / test_key
    key_metadata_path = kvs_client.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert key_metadata_path.exists()

    # Check file content
    content = key_path.read_text(encoding='utf-8')
    assert content == test_value

    # Check record metadata file
    with key_metadata_path.open() as f:
        metadata = json.load(f)
        assert metadata['key'] == test_key
        assert metadata['content_type'] == 'text/plain; charset=utf-8'
        assert metadata['size'] == len(test_value.encode('utf-8'))


async def test_binary_data_persistence(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_client.set_value(key=test_key, value=test_value)

    # Verify binary file exists
    key_path = kvs_client.path_to_kvs / test_key
    assert key_path.exists()

    # Verify binary content is preserved
    content = key_path.read_bytes()
    assert content == test_value

    # Verify retrieval works correctly
    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'


async def test_json_serialization_to_file(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that JSON objects are properly serialized to files."""
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_client.set_value(key=test_key, value=test_value)

    # Check if file content is valid JSON
    key_path = kvs_client.path_to_kvs / test_key
    with key_path.open() as f:
        file_content = json.load(f)
        assert file_content == test_value


async def test_file_deletion_on_value_delete(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that deleting a value removes its files from disk."""
    test_key = 'test-delete'
    test_value = 'Delete me'

    # Set a value
    await kvs_client.set_value(key=test_key, value=test_value)

    # Verify files exist
    key_path = kvs_client.path_to_kvs / test_key
    metadata_path = kvs_client.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert metadata_path.exists()

    # Delete the value
    await kvs_client.delete_value(key=test_key)

    # Verify files were deleted
    assert not key_path.exists()
    assert not metadata_path.exists()


async def test_drop_removes_directory(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that drop removes the entire store directory from disk."""
    await kvs_client.set_value(key='test', value='test-value')

    assert kvs_client.path_to_kvs.exists()

    # Drop the store
    await kvs_client.drop()

    assert not kvs_client.path_to_kvs.exists()


async def test_metadata_file_updates(kvs_client: FileSystemKeyValueStoreClient) -> None:
    """Test that read/write operations properly update metadata file timestamps."""
    # Record initial timestamps
    metadata = await kvs_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_client.get_value(key='nonexistent')

    # Verify accessed timestamp was updated
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_client.set_value(key='test', value='test-value')

    # Verify modified timestamp was updated
    metadata = await kvs_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same KVS."""
    storage_client = FileSystemStorageClient()

    # Create KVS and add data
    original_client = await storage_client.create_kvs_client(
        name='persistence-test',
        configuration=configuration,
    )

    test_key = 'persistent-key'
    test_value = 'persistent-value'
    await original_client.set_value(key=test_key, value=test_value)

    kvs_id = (await original_client.get_metadata()).id

    # Reopen by ID and verify data persists
    reopened_client = await storage_client.create_kvs_client(
        id=kvs_id,
        configuration=configuration,
    )

    record = await reopened_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value

    await reopened_client.drop()
