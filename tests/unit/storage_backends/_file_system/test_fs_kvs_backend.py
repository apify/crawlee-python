from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_backends import FileSystemStorageBackend
from crawlee.storage_backends.models import KeyValueStoreMetadata, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_backends._file_system import FileSystemKeyValueStoreBackend


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    return Configuration(
        storage_dir=str(tmp_path),
    )


@pytest.fixture
async def kvs_backend(configuration: Configuration) -> AsyncGenerator[FileSystemKeyValueStoreBackend, None]:
    """A fixture for a file system key-value store backend."""
    backend = await FileSystemStorageBackend().create_kvs_backend(name='test-kvs', configuration=configuration)
    yield backend
    await backend.drop()


async def test_file_and_directory_creation(configuration: Configuration) -> None:
    """Test that file system KVS creates proper files and directories."""
    backend = await FileSystemStorageBackend().create_kvs_backend(name='new-kvs', configuration=configuration)

    # Verify files were created
    assert backend.path_to_kvs.exists()
    assert backend.path_to_metadata.exists()

    # Verify metadata file structure
    with backend.path_to_metadata.open() as f:
        metadata = json.load(f)
        assert metadata['id'] == (await backend.get_metadata()).id
        assert metadata['name'] == 'new-kvs'

    await backend.drop()


@pytest.mark.parametrize(
    'invalid_value',
    [
        pytest.param('../outside', id='parent-ref'),
        pytest.param('..', id='bare-parent'),
        pytest.param('/abs/outside', id='absolute-path'),
    ],
)
async def test_open_rejects_invalid_name_or_alias(
    configuration: Configuration, tmp_path: Path, invalid_value: str
) -> None:
    """The low-level backend must reject names/aliases that resolve outside the storage directory.

    This covers direct usage of the storage backend, which bypasses the high-level validation.
    """
    storage_backend = FileSystemStorageBackend()

    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        await storage_backend.create_kvs_backend(alias=invalid_value, configuration=configuration)

    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        await storage_backend.create_kvs_backend(name=invalid_value, configuration=configuration)

    # Nothing should have been written outside the configured storage directory.
    assert not (tmp_path / 'outside').exists()


async def test_value_file_creation_and_content(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that values are properly persisted to files with correct content and metadata."""
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_backend.set_value(key=test_key, value=test_value)

    # Check if the files were created
    key_path = kvs_backend.path_to_kvs / test_key
    key_metadata_path = kvs_backend.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert key_metadata_path.exists()

    # Check file content
    content = key_path.read_text(encoding='utf-8')
    assert content == test_value

    # Check record metadata file
    with key_metadata_path.open() as f:
        metadata = json.load(f)
        assert metadata['key'] == test_key
        assert metadata['contentType'] == 'text/plain; charset=utf-8'
        assert metadata['size'] == len(test_value.encode('utf-8'))


async def test_binary_data_persistence(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_backend.set_value(key=test_key, value=test_value)

    # Verify binary file exists
    key_path = kvs_backend.path_to_kvs / test_key
    assert key_path.exists()

    # Verify binary content is preserved
    content = key_path.read_bytes()
    assert content == test_value

    # Verify retrieval works correctly
    record = await kvs_backend.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'


async def test_json_serialization_to_file(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that JSON objects are properly serialized to files."""
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_backend.set_value(key=test_key, value=test_value)

    # Check if file content is valid JSON
    key_path = kvs_backend.path_to_kvs / test_key
    with key_path.open() as f:
        file_content = json.load(f)
        assert file_content == test_value


async def test_file_deletion_on_value_delete(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that deleting a value removes its files from disk."""
    test_key = 'test-delete'
    test_value = 'Delete me'

    # Set a value
    await kvs_backend.set_value(key=test_key, value=test_value)

    # Verify files exist
    key_path = kvs_backend.path_to_kvs / test_key
    metadata_path = kvs_backend.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert metadata_path.exists()

    # Delete the value
    await kvs_backend.delete_value(key=test_key)

    # Verify files were deleted
    assert not key_path.exists()
    assert not metadata_path.exists()


async def test_drop_removes_directory(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that drop removes the entire store directory from disk."""
    await kvs_backend.set_value(key='test', value='test-value')

    assert kvs_backend.path_to_kvs.exists()

    # Drop the store
    await kvs_backend.drop()

    assert not kvs_backend.path_to_kvs.exists()


async def test_metadata_file_updates(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that read/write operations properly update metadata file timestamps."""
    # Record initial timestamps
    metadata = await kvs_backend.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await kvs_backend.get_value(key='nonexistent')

    # Verify accessed timestamp was updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await kvs_backend.set_value(key='test', value='test-value')

    # Verify modified timestamp was updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same KVS."""
    storage_backend = FileSystemStorageBackend()

    # Create KVS and add data
    original_client = await storage_backend.create_kvs_backend(name='persistence-test', configuration=configuration)

    test_key = 'persistent-key'
    test_value = 'persistent-value'
    await original_client.set_value(key=test_key, value=test_value)

    kvs_id = (await original_client.get_metadata()).id

    # Reopen by ID and verify data persists
    reopened_client = await storage_backend.create_kvs_backend(
        id=kvs_id,
    )

    record = await reopened_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value

    await reopened_client.drop()


async def test_snake_case_files_from_v1_are_still_read(configuration: Configuration) -> None:
    """Store and record metadata files with snake_case keys, as Crawlee v1 wrote them, still load."""
    storage_backend = FileSystemStorageBackend()
    backend = await storage_backend.create_kvs_backend(name='snake-case-kvs', configuration=configuration)
    await backend.set_value(key='key', value='value')
    kvs_id = (await backend.get_metadata()).id

    # Rewrite every metadata file the way v1 serialized it, without aliases.
    for path in backend.path_to_kvs.glob(f'*{METADATA_FILENAME}'):
        model = KeyValueStoreMetadata if path == backend.path_to_metadata else KeyValueStoreRecordMetadata
        snake_case_data = model.model_validate_json(path.read_text()).model_dump(mode='json', by_alias=False)
        path.write_text(json.dumps(snake_case_data))

    reopened_client = await storage_backend.create_kvs_backend(id=kvs_id, configuration=configuration)
    assert (await reopened_client.get_metadata()).id == kvs_id

    record = await reopened_client.get_value(key='key')
    assert record is not None
    assert record.value == 'value'
    assert record.content_type == 'text/plain; charset=utf-8'
    assert [key.key async for key in reopened_client.iterate_keys()] == ['key']

    await reopened_client.drop()
