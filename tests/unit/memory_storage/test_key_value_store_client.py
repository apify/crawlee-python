from __future__ import annotations

import asyncio
import base64
import json
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.data_processing import maybe_parse_body
from crawlee._utils.file import json_dumps

if TYPE_CHECKING:
    from pathlib import Path

    from crawlee.memory_storage import KeyValueStoreClient, MemoryStorageClient

TINY_PNG = base64.b64decode(
    s='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVQYV2NgYAAAAAMAAWgmWQ0AAAAASUVORK5CYII=',
)
TINY_BYTES = b'\x12\x34\x56\x78\x90\xab\xcd\xef'
TINY_DATA = {'a': 'b'}
TINY_TEXT = 'abcd'


@pytest.fixture()
async def key_value_store_client(memory_storage_client: MemoryStorageClient) -> KeyValueStoreClient:
    key_value_stores_client = memory_storage_client.key_value_stores()
    kvs_info = await key_value_stores_client.get_or_create(name='test')
    return memory_storage_client.key_value_store(kvs_info.id)


async def test_nonexistent(memory_storage_client: MemoryStorageClient) -> None:
    kvs_client = memory_storage_client.key_value_store(key_value_store_id='nonexistent-id')
    assert await kvs_client.get() is None

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.update(name='test-update')

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.list_keys()

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.set_record('test', {'abc': 123})

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.get_record('test')

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.get_record_as_bytes('test')

    with pytest.raises(ValueError, match='Key-value store with id "nonexistent-id" does not exist.'):
        await kvs_client.delete_record('test')

    await kvs_client.delete()


async def test_not_implemented(key_value_store_client: KeyValueStoreClient) -> None:
    with pytest.raises(NotImplementedError, match='This method is not supported in local memory storage.'):
        await key_value_store_client.stream_record('test')


async def test_get(key_value_store_client: KeyValueStoreClient) -> None:
    await asyncio.sleep(0.1)
    info = await key_value_store_client.get()
    assert info is not None
    assert info.id == key_value_store_client.id
    assert info.accessed_at != info.created_at


async def test_update(key_value_store_client: KeyValueStoreClient) -> None:
    new_kvs_name = 'test-update'
    await key_value_store_client.set_record('test', {'abc': 123})
    old_kvs_info = await key_value_store_client.get()
    assert old_kvs_info is not None
    old_kvs_directory = os.path.join(
        key_value_store_client._memory_storage_client.key_value_stores_directory, old_kvs_info.name
    )
    new_kvs_directory = os.path.join(
        key_value_store_client._memory_storage_client.key_value_stores_directory, new_kvs_name
    )
    assert os.path.exists(os.path.join(old_kvs_directory, 'test.json')) is True
    assert os.path.exists(os.path.join(new_kvs_directory, 'test.json')) is False

    await asyncio.sleep(0.1)
    updated_kvs_info = await key_value_store_client.update(name=new_kvs_name)
    assert os.path.exists(os.path.join(old_kvs_directory, 'test.json')) is False
    assert os.path.exists(os.path.join(new_kvs_directory, 'test.json')) is True
    # Only modifiedAt and accessedAt should be different
    assert old_kvs_info.created_at == updated_kvs_info.created_at
    assert old_kvs_info.modified_at != updated_kvs_info.modified_at
    assert old_kvs_info.accessed_at != updated_kvs_info.accessed_at

    # Should fail with the same name
    with pytest.raises(ValueError, match='Key-value store with name "test-update" already exists.'):
        await key_value_store_client.update(name=new_kvs_name)


async def test_delete(key_value_store_client: KeyValueStoreClient) -> None:
    await key_value_store_client.set_record('test', {'abc': 123})
    kvs_info = await key_value_store_client.get()
    assert kvs_info is not None
    kvs_directory = os.path.join(
        key_value_store_client._memory_storage_client.key_value_stores_directory, kvs_info.name
    )
    assert os.path.exists(os.path.join(kvs_directory, 'test.json')) is True
    await key_value_store_client.delete()
    assert os.path.exists(os.path.join(kvs_directory, 'test.json')) is False
    # Does not crash when called again
    await key_value_store_client.delete()


async def test_list_keys_empty(key_value_store_client: KeyValueStoreClient) -> None:
    keys = await key_value_store_client.list_keys()
    assert len(keys.items) == 0
    assert keys.count == 0
    assert keys.is_truncated is False


async def test_list_keys(key_value_store_client: KeyValueStoreClient) -> None:
    record_count = 4
    used_limit = 2
    used_exclusive_start_key = 'a'
    await key_value_store_client.set_record('b', 'test')
    await key_value_store_client.set_record('a', 'test')
    await key_value_store_client.set_record('d', 'test')
    await key_value_store_client.set_record('c', 'test')

    # Default settings
    keys = await key_value_store_client.list_keys()
    assert keys.items[0].key == 'a'
    assert keys.items[3].key == 'd'
    assert keys.count == record_count
    assert keys.is_truncated is False
    # Test limit
    keys_limit_2 = await key_value_store_client.list_keys(limit=used_limit)
    assert keys_limit_2.count == record_count
    assert keys_limit_2.limit == used_limit
    assert keys_limit_2.items[1].key == 'b'
    # Test exclusive start key
    keys_exclusive_start = await key_value_store_client.list_keys(exclusive_start_key=used_exclusive_start_key, limit=2)
    assert keys_exclusive_start.exclusive_start_key == used_exclusive_start_key
    assert keys_exclusive_start.is_truncated is True
    assert keys_exclusive_start.next_exclusive_start_key == 'c'
    assert keys_exclusive_start.items[0].key == 'b'
    assert keys_exclusive_start.items[-1].key == keys_exclusive_start.next_exclusive_start_key


async def test_get_and_set_record(tmp_path: Path, key_value_store_client: KeyValueStoreClient) -> None:
    # Test setting dict record
    dict_record_key = 'test-dict'
    await key_value_store_client.set_record(dict_record_key, {'test': 123})
    dict_record_info = await key_value_store_client.get_record(dict_record_key)
    assert dict_record_info is not None
    assert 'application/json' in str(dict_record_info.content_type)
    assert dict_record_info.value['test'] == 123

    # Test setting str record
    str_record_key = 'test-str'
    await key_value_store_client.set_record(str_record_key, 'test')
    str_record_info = await key_value_store_client.get_record(str_record_key)
    assert str_record_info is not None
    assert 'text/plain' in str(str_record_info.content_type)
    assert str_record_info.value == 'test'

    # Test setting explicit json record but use str as value, i.e. json dumps is skipped
    explicit_json_key = 'test-json'
    await key_value_store_client.set_record(explicit_json_key, '{"test": "explicit string"}', 'application/json')
    bytes_record_info = await key_value_store_client.get_record(explicit_json_key)
    assert bytes_record_info is not None
    assert 'application/json' in str(bytes_record_info.content_type)
    assert bytes_record_info.value['test'] == 'explicit string'

    # Test using bytes
    bytes_key = 'test-json'
    bytes_value = b'testing bytes set_record'
    await key_value_store_client.set_record(bytes_key, bytes_value, 'unknown')
    bytes_record_info = await key_value_store_client.get_record(bytes_key)
    assert bytes_record_info is not None
    assert 'unknown' in str(bytes_record_info.content_type)
    assert bytes_record_info.value == bytes_value
    assert bytes_record_info.value.decode('utf-8') == bytes_value.decode('utf-8')

    # Test using file descriptor
    with open(os.path.join(tmp_path, 'test.json'), 'w+', encoding='utf-8') as f:  # noqa: ASYNC101
        f.write('Test')
        with pytest.raises(NotImplementedError, match='File-like values are not supported in local memory storage'):
            await key_value_store_client.set_record('file', f)


async def test_get_record_as_bytes(key_value_store_client: KeyValueStoreClient) -> None:
    record_key = 'test'
    record_value = 'testing'
    await key_value_store_client.set_record(record_key, record_value)
    record_info = await key_value_store_client.get_record_as_bytes(record_key)
    assert record_info is not None
    assert record_info.value == record_value.encode('utf-8')


async def test_delete_record(key_value_store_client: KeyValueStoreClient) -> None:
    record_key = 'test'
    await key_value_store_client.set_record(record_key, 'test')
    await key_value_store_client.delete_record(record_key)
    # Does not crash when called again
    await key_value_store_client.delete_record(record_key)


@pytest.mark.parametrize(
    'test_case',
    [
        {
            'input': {'key': 'image', 'value': TINY_PNG, 'contentType': None},
            'expectedOutput': {'filename': 'image', 'key': 'image', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {'key': 'image', 'value': TINY_PNG, 'contentType': 'image/png'},
            'expectedOutput': {'filename': 'image.png', 'key': 'image', 'contentType': 'image/png'},
        },
        {
            'input': {'key': 'image.png', 'value': TINY_PNG, 'contentType': None},
            'expectedOutput': {'filename': 'image.png', 'key': 'image.png', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {'key': 'image.png', 'value': TINY_PNG, 'contentType': 'image/png'},
            'expectedOutput': {'filename': 'image.png', 'key': 'image.png', 'contentType': 'image/png'},
        },
        {
            'input': {'key': 'data', 'value': TINY_DATA, 'contentType': None},
            'expectedOutput': {'filename': 'data.json', 'key': 'data', 'contentType': 'application/json'},
        },
        {
            'input': {'key': 'data', 'value': TINY_DATA, 'contentType': 'application/json'},
            'expectedOutput': {'filename': 'data.json', 'key': 'data', 'contentType': 'application/json'},
        },
        {
            'input': {'key': 'data.json', 'value': TINY_DATA, 'contentType': None},
            'expectedOutput': {'filename': 'data.json', 'key': 'data.json', 'contentType': 'application/json'},
        },
        {
            'input': {'key': 'data.json', 'value': TINY_DATA, 'contentType': 'application/json'},
            'expectedOutput': {'filename': 'data.json', 'key': 'data.json', 'contentType': 'application/json'},
        },
        {
            'input': {'key': 'text', 'value': TINY_TEXT, 'contentType': None},
            'expectedOutput': {'filename': 'text.txt', 'key': 'text', 'contentType': 'text/plain'},
        },
        {
            'input': {'key': 'text', 'value': TINY_TEXT, 'contentType': 'text/plain'},
            'expectedOutput': {'filename': 'text.txt', 'key': 'text', 'contentType': 'text/plain'},
        },
        {
            'input': {'key': 'text.txt', 'value': TINY_TEXT, 'contentType': None},
            'expectedOutput': {'filename': 'text.txt', 'key': 'text.txt', 'contentType': 'text/plain'},
        },
        {
            'input': {'key': 'text.txt', 'value': TINY_TEXT, 'contentType': 'text/plain'},
            'expectedOutput': {'filename': 'text.txt', 'key': 'text.txt', 'contentType': 'text/plain'},
        },
    ],
)
async def test_writes_correct_metadata(memory_storage_client: MemoryStorageClient, test_case: dict) -> None:
    test_input = test_case['input']
    expected_output = test_case['expectedOutput']
    key_value_store_name = crypto_random_object_id()

    # Write the input data to the key-value store
    store_details = await memory_storage_client.key_value_stores().get_or_create(name=key_value_store_name)
    key_value_store_client = memory_storage_client.key_value_store(store_details.id)
    await key_value_store_client.set_record(
        test_input['key'], test_input['value'], content_type=test_input['contentType']
    )

    # Check that everything was written correctly, both the data and metadata
    storage_path = os.path.join(memory_storage_client.key_value_stores_directory, key_value_store_name)
    item_path = os.path.join(storage_path, expected_output['filename'])
    metadata_path = os.path.join(storage_path, expected_output['filename'] + '.__metadata__.json')

    assert os.path.exists(item_path)
    assert os.path.exists(metadata_path)

    with open(item_path, 'rb') as item_file:  # noqa: ASYNC101
        actual_value = maybe_parse_body(item_file.read(), expected_output['contentType'])
        assert actual_value == test_input['value']

    with open(metadata_path, encoding='utf-8') as metadata_file:  # noqa: ASYNC101
        metadata = json.load(metadata_file)
        assert metadata['key'] == expected_output['key']
        assert expected_output['contentType'] in metadata['contentType']


@pytest.mark.parametrize(
    'test_case',
    [
        {
            'input': {'filename': 'image', 'value': TINY_PNG, 'metadata': None},
            'expectedOutput': {'key': 'image', 'filename': 'image', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {'filename': 'image.png', 'value': TINY_PNG, 'metadata': None},
            'expectedOutput': {'key': 'image', 'filename': 'image.png', 'contentType': 'image/png'},
        },
        {
            'input': {
                'filename': 'image',
                'value': TINY_PNG,
                'metadata': {'key': 'image', 'contentType': 'application/octet-stream'},
            },
            'expectedOutput': {'key': 'image', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {'filename': 'image', 'value': TINY_PNG, 'metadata': {'key': 'image', 'contentType': 'image/png'}},
            'expectedOutput': {'key': 'image', 'filename': 'image', 'contentType': 'image/png'},
        },
        {
            'input': {
                'filename': 'image.png',
                'value': TINY_PNG,
                'metadata': {'key': 'image.png', 'contentType': 'application/octet-stream'},
            },
            'expectedOutput': {'key': 'image.png', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {
                'filename': 'image.png',
                'value': TINY_PNG,
                'metadata': {'key': 'image.png', 'contentType': 'image/png'},
            },
            'expectedOutput': {'key': 'image.png', 'contentType': 'image/png'},
        },
        {
            'input': {
                'filename': 'image.png',
                'value': TINY_PNG,
                'metadata': {'key': 'image', 'contentType': 'image/png'},
            },
            'expectedOutput': {'key': 'image', 'contentType': 'image/png'},
        },
        {
            'input': {'filename': 'input', 'value': TINY_BYTES, 'metadata': None},
            'expectedOutput': {'key': 'input', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {'filename': 'input.json', 'value': TINY_DATA, 'metadata': None},
            'expectedOutput': {'key': 'input', 'contentType': 'application/json'},
        },
        {
            'input': {'filename': 'input.txt', 'value': TINY_TEXT, 'metadata': None},
            'expectedOutput': {'key': 'input', 'contentType': 'text/plain'},
        },
        {
            'input': {'filename': 'input.bin', 'value': TINY_BYTES, 'metadata': None},
            'expectedOutput': {'key': 'input', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {
                'filename': 'input',
                'value': TINY_BYTES,
                'metadata': {'key': 'input', 'contentType': 'application/octet-stream'},
            },
            'expectedOutput': {'key': 'input', 'contentType': 'application/octet-stream'},
        },
        {
            'input': {
                'filename': 'input.json',
                'value': TINY_DATA,
                'metadata': {'key': 'input', 'contentType': 'application/json'},
            },
            'expectedOutput': {'key': 'input', 'contentType': 'application/json'},
        },
        {
            'input': {
                'filename': 'input.txt',
                'value': TINY_TEXT,
                'metadata': {'key': 'input', 'contentType': 'text/plain'},
            },
            'expectedOutput': {'key': 'input', 'contentType': 'text/plain'},
        },
        {
            'input': {
                'filename': 'input.bin',
                'value': TINY_BYTES,
                'metadata': {'key': 'input', 'contentType': 'application/octet-stream'},
            },
            'expectedOutput': {'key': 'input', 'contentType': 'application/octet-stream'},
        },
    ],
)
async def test_reads_correct_metadata(memory_storage_client: MemoryStorageClient, test_case: dict) -> None:
    test_input = test_case['input']
    expected_output = test_case['expectedOutput']
    key_value_store_name = crypto_random_object_id()

    # Ensure the directory for the store exists
    storage_path = os.path.join(memory_storage_client.key_value_stores_directory, key_value_store_name)
    os.makedirs(storage_path, exist_ok=True)

    store_metadata = {
        'id': crypto_random_object_id(),
        'name': None,
        'accessedAt': datetime.now(timezone.utc),
        'createdAt': datetime.now(timezone.utc),
        'modifiedAt': datetime.now(timezone.utc),
        'userId': '1',
    }

    # Write the store metadata to disk
    store_metadata_path = os.path.join(storage_path, '__metadata__.json')
    with open(store_metadata_path, mode='wb') as store_metadata_file:  # noqa: ASYNC101
        s = await json_dumps(store_metadata)
        store_metadata_file.write(s.encode('utf-8'))

    # Write the test input item to the disk
    item_path = os.path.join(storage_path, test_input['filename'])
    with open(item_path, 'wb') as item_file:  # noqa: ASYNC101
        if isinstance(test_input['value'], bytes):
            item_file.write(test_input['value'])
        elif isinstance(test_input['value'], str):
            item_file.write(test_input['value'].encode('utf-8'))
        else:
            s = await json_dumps(test_input['value'])
            item_file.write(s.encode('utf-8'))

    # Optionally write the metadata to disk if there is some
    if test_input['metadata'] is not None:
        metadata_path = os.path.join(storage_path, test_input['filename'] + '.__metadata__.json')
        with open(metadata_path, 'w', encoding='utf-8') as metadata_file:  # noqa: ASYNC101
            s = await json_dumps(
                {
                    'key': test_input['metadata']['key'],
                    'contentType': test_input['metadata']['contentType'],
                }
            )
            metadata_file.write(s)

    # Create the key-value store client to load the items from disk
    store_details = await memory_storage_client.key_value_stores().get_or_create(name=key_value_store_name)
    key_value_store_client = memory_storage_client.key_value_store(store_details.id)

    # Read the item from the store and check if it is as expected
    actual_record = await key_value_store_client.get_record(expected_output['key'])
    assert actual_record is not None

    assert actual_record.key == expected_output['key']
    assert actual_record.content_type == expected_output['contentType']
    assert actual_record.value == test_input['value']
