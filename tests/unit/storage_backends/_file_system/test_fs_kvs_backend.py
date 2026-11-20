from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest

from crawlee._consts import METADATA_FILENAME
from crawlee.configuration import Configuration
from crawlee.storage_backends import FileSystemStorageBackend
from crawlee.storage_backends.models import KeyValueStoreMetadata, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from crawlee.storage_backends._file_system import FileSystemKeyValueStoreBackend

_BACKEND_MODULE = 'crawlee.storage_backends._file_system._key_value_store_backend'


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
    test_value = b'Hello, world!'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain; charset=utf-8')

    # Check if the files were created
    key_path = kvs_backend.path_to_kvs / test_key
    key_metadata_path = kvs_backend.path_to_kvs / f'{test_key}.{METADATA_FILENAME}'
    assert key_path.exists()
    assert key_metadata_path.exists()

    # Check file content
    content = key_path.read_bytes()
    assert content == test_value

    # Check record metadata file
    with key_metadata_path.open() as f:
        metadata = json.load(f)
        assert metadata['key'] == test_key
        assert metadata['contentType'] == 'text/plain; charset=utf-8'
        assert metadata['size'] == len(test_value)


async def test_binary_data_persistence(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='application/octet-stream')

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


async def test_file_deletion_on_value_delete(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """Test that deleting a value removes its files from disk."""
    test_key = 'test-delete'
    test_value = b'Delete me'

    # Set a value
    await kvs_backend.set_value(key=test_key, value=test_value, content_type='text/plain')

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
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

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
    await kvs_backend.set_value(key='test', value=b'test-value', content_type='text/plain')

    # Verify modified timestamp was updated
    metadata = await kvs_backend.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read


async def test_data_persistence_across_reopens(configuration: Configuration) -> None:
    """Test that data persists correctly when reopening the same KVS."""
    storage_backend = FileSystemStorageBackend()

    # Create KVS and add data
    original_backend = await storage_backend.create_kvs_backend(name='persistence-test', configuration=configuration)

    test_key = 'persistent-key'
    test_value = b'persistent-value'
    await original_backend.set_value(key=test_key, value=test_value, content_type='text/plain')

    kvs_id = (await original_backend.get_metadata()).id

    # Reopen by ID and verify data persists
    reopened_backend = await storage_backend.create_kvs_backend(
        id=kvs_id,
    )

    record = await reopened_backend.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value

    await reopened_backend.drop()


async def test_snake_case_files_from_v1_are_still_read(configuration: Configuration) -> None:
    """Store and record metadata files with snake_case keys, as Crawlee v1 wrote them, still load."""
    storage_backend = FileSystemStorageBackend()
    backend = await storage_backend.create_kvs_backend(name='snake-case-kvs', configuration=configuration)
    await backend.set_value(key='key', value=b'value', content_type='text/plain; charset=utf-8')
    kvs_id = (await backend.get_metadata()).id

    # Rewrite every metadata file the way v1 serialized it, without aliases.
    for path in backend.path_to_kvs.glob(f'*{METADATA_FILENAME}'):
        model = KeyValueStoreMetadata if path == backend.path_to_metadata else KeyValueStoreRecordMetadata
        snake_case_data = model.model_validate_json(path.read_text()).model_dump(mode='json', by_alias=False)
        path.write_text(json.dumps(snake_case_data))

    reopened_backend = await storage_backend.create_kvs_backend(id=kvs_id, configuration=configuration)
    assert (await reopened_backend.get_metadata()).id == kvs_id

    record = await reopened_backend.get_value(key='key')
    assert record is not None
    assert record.value == b'value'
    assert record.content_type == 'text/plain; charset=utf-8'
    assert [item.key for item in (await reopened_backend.list_keys()).items] == ['key']

    await reopened_backend.drop()


async def test_list_keys_pages_follow_key_order(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """The pages follow the order of the keys, which differs from the order of their encoded file names."""
    for key in ['ab', 'a{', 'a b']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')

    listed_keys = list[str]()
    exclusive_start_key = None
    while True:
        page = await kvs_backend.list_keys(exclusive_start_key=exclusive_start_key, limit=1)
        listed_keys.extend(item.key for item in page.items)
        if page.next_exclusive_start_key is None:
            break
        exclusive_start_key = page.next_exclusive_start_key

    assert listed_keys == ['a b', 'ab', 'a{']


@pytest.mark.parametrize(
    'metadata_content',
    [
        pytest.param('{not json', id='invalid JSON'),
        pytest.param('{}', id='invalid schema'),
    ],
)
async def test_list_keys_skips_record_with_invalid_metadata(
    kvs_backend: FileSystemKeyValueStoreBackend, metadata_content: str
) -> None:
    """A record whose metadata file doesn't hold valid record metadata is left out of the listing."""
    await kvs_backend.set_value(key='broken', value=b'value', content_type='text/plain')
    await kvs_backend.set_value(key='valid', value=b'value', content_type='text/plain')
    broken_metadata_path = kvs_backend.path_to_kvs / f'broken.{METADATA_FILENAME}'
    broken_metadata_path.write_text(metadata_content, encoding='utf-8')

    page = await kvs_backend.list_keys()

    assert [item.key for item in page.items] == ['valid']
    assert page.count == 1
    assert page.next_exclusive_start_key is None


async def test_list_keys_continues_without_reading_directory_again(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """A listing that continues from the last page reads the directory once and still sees the backend's own writes."""
    for key in ['a', 'b', 'c']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')

    with patch.object(kvs_backend, '_read_sorted_keys', wraps=kvs_backend._read_sorted_keys) as read_sorted_keys:
        first_page = await kvs_backend.list_keys(limit=1)
        await kvs_backend.set_value(key='d', value=b'value', content_type='text/plain')
        await kvs_backend.delete_value(key='b')
        second_page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key, limit=2)

    assert [item.key for item in first_page.items] == ['a']
    assert [item.key for item in second_page.items] == ['c', 'd']
    assert second_page.next_exclusive_start_key is None
    assert read_sorted_keys.call_count == 1


@pytest.mark.parametrize(
    'exclusive_start_key',
    [
        pytest.param(None, id='from the first key'),
        pytest.param('0', id='from a key other than the last cursor'),
    ],
)
async def test_list_keys_sees_files_written_outside_the_backend(
    kvs_backend: FileSystemKeyValueStoreBackend, exclusive_start_key: str | None
) -> None:
    """A new listing reads the directory again, so it sees a record written there by something else."""
    for key in ['a', 'c']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    # A listing abandoned before its last page keeps the keys it read.
    await kvs_backend.list_keys(limit=1)

    (kvs_backend.path_to_kvs / 'b').write_bytes(b'value')
    metadata = KeyValueStoreRecordMetadata(key='b', content_type='text/plain', size=5)
    (kvs_backend.path_to_kvs / f'b.{METADATA_FILENAME}').write_text(metadata.model_dump_json(by_alias=True))

    page = await kvs_backend.list_keys(exclusive_start_key=exclusive_start_key)

    assert [item.key for item in page.items] == ['a', 'b', 'c']


async def test_purge_empties_the_listing(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """A listing that continues after a purge lists no keys and has no next page."""
    for key in ['a', 'b', 'c']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    first_page = await kvs_backend.list_keys(limit=1)

    await kvs_backend.purge()

    page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key, limit=1)
    assert page.items == []
    assert page.next_exclusive_start_key is None


async def test_list_keys_sees_record_of_cancelled_write(
    kvs_backend: FileSystemKeyValueStoreBackend,
) -> None:
    """A listing that continues after a cancelled `set_value` lists the record its thread still wrote."""
    for key in ['a', 'b']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    first_page = await kvs_backend.list_keys(limit=1)

    with (
        patch(f'{_BACKEND_MODULE}.atomic_write', AsyncMock(side_effect=asyncio.CancelledError)),
        pytest.raises(asyncio.CancelledError),
    ):
        await kvs_backend.set_value(key='c', value=b'value', content_type='text/plain')
    (kvs_backend.path_to_kvs / 'c').write_bytes(b'value')
    metadata = KeyValueStoreRecordMetadata(key='c', content_type='text/plain', size=5)
    (kvs_backend.path_to_kvs / f'c.{METADATA_FILENAME}').write_text(metadata.model_dump_json(by_alias=True))

    page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key)

    assert [item.key for item in page.items] == ['b', 'c']


async def test_list_keys_skips_record_of_cancelled_delete(
    kvs_backend: FileSystemKeyValueStoreBackend,
) -> None:
    """A listing that continues after a cancelled `delete_value` leaves out the record its thread still deleted."""
    for key in ['a', 'b', 'c']:
        await kvs_backend.set_value(key=key, value=b'value', content_type='text/plain')
    first_page = await kvs_backend.list_keys(limit=1)

    with (
        patch('asyncio.to_thread', AsyncMock(side_effect=asyncio.CancelledError)),
        pytest.raises(asyncio.CancelledError),
    ):
        await kvs_backend.delete_value(key='b')
    (kvs_backend.path_to_kvs / 'b').unlink()
    (kvs_backend.path_to_kvs / f'b.{METADATA_FILENAME}').unlink()

    page = await kvs_backend.list_keys(exclusive_start_key=first_page.next_exclusive_start_key, limit=1)

    assert [item.key for item in page.items] == ['c']
    assert page.next_exclusive_start_key is None


async def test_list_keys_waiting_for_drop_lists_no_keys(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """A listing that waits for a concurrent `drop` to finish lists no keys."""
    await kvs_backend.set_value(key='key', value=b'value', content_type='text/plain')

    async with kvs_backend._lock:
        drop_task = asyncio.create_task(kvs_backend.drop())
        list_keys_task = asyncio.create_task(kvs_backend.list_keys())
        await asyncio.sleep(0)

    await drop_task
    page = await list_keys_task

    assert page.items == []


async def test_list_keys_without_directory_returns_empty_page(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """A listing of a store without a directory returns an empty last page with the `limit` and start key it got."""
    await kvs_backend.drop()

    page = await kvs_backend.list_keys(exclusive_start_key='key', limit=10)

    assert page.items == []
    assert page.count == 0
    assert page.limit == 10
    assert page.exclusive_start_key == 'key'
    assert page.next_exclusive_start_key is None


async def test_get_values_reads_records_in_one_pass(kvs_backend: FileSystemKeyValueStoreBackend) -> None:
    """`get_values` reads the records of the keys that have one, and updates the store metadata once."""
    await kvs_backend.set_value(key='first', value=b'a', content_type='text/plain')
    await kvs_backend.set_value(key='second', value=b'b', content_type='application/octet-stream')

    with patch.object(kvs_backend, '_update_metadata', wraps=kvs_backend._update_metadata) as update_metadata:
        records = await kvs_backend.get_values(keys=['second', 'nonexistent', 'first'])

    assert {key: (record.value, record.content_type) for key, record in records.items()} == {
        'second': (b'b', 'application/octet-stream'),
        'first': (b'a', 'text/plain'),
    }
    assert update_metadata.call_count == 1
