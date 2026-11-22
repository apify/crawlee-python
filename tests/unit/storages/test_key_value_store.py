from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field, ValidationError

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.events import Event, EventPersistStateData
from crawlee.storage_backends import FileSystemStorageBackend, MemoryStorageBackend, SqlStorageBackend, StorageBackend
from crawlee.storage_backends.models import KeyValueStoreKeysListPage, KeyValueStoreRecordMetadata
from crawlee.storages import KeyValueStore
from crawlee.storages._storage_instance_manager import StorageInstanceManager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path


class _Product(BaseModel):
    name: str
    price: float


class _AliasedProduct(BaseModel):
    product_name: str = Field(alias='productName')


@pytest.fixture
async def kvs(
    storage_backend: StorageBackend,
) -> AsyncGenerator[KeyValueStore, None]:
    """Fixture that provides a key-value store instance for each test."""
    kvs = await KeyValueStore.open(
        storage_backend=storage_backend,
    )

    yield kvs
    await kvs.drop()


async def test_open_creates_new_kvs(
    storage_backend: StorageBackend,
) -> None:
    """Test that open() creates a new key-value store with proper metadata."""
    kvs = await KeyValueStore.open(
        name='new-kvs',
        storage_backend=storage_backend,
    )

    # Verify key-value store properties
    assert kvs.id is not None
    assert kvs.name == 'new-kvs'

    await kvs.drop()


async def test_open_existing_kvs(
    kvs: KeyValueStore,
    storage_backend: StorageBackend,
) -> None:
    """Test that open() loads an existing key-value store correctly."""
    # Open the same key-value store again
    reopened_kvs = await KeyValueStore.open(
        name=kvs.name,
        storage_backend=storage_backend,
    )

    # Verify key-value store properties
    assert kvs.id == reopened_kvs.id
    assert kvs.name == reopened_kvs.name

    # Verify they are the same object (from cache)
    assert id(kvs) == id(reopened_kvs)


async def test_open_with_id_and_name(
    storage_backend: StorageBackend,
) -> None:
    """Test that open() raises an error when both id and name are provided."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "id", "name".',
    ):
        await KeyValueStore.open(
            id='some-id',
            name='some-name',
            storage_backend=storage_backend,
        )


async def test_open_by_id(
    storage_backend: StorageBackend,
) -> None:
    """Test opening a key-value store by its ID."""
    # First create a key-value store by name
    kvs1 = await KeyValueStore.open(
        name='kvs-by-id-test',
        storage_backend=storage_backend,
    )

    # Add some data to identify it
    await kvs1.set_value('test_key', {'test': 'opening_by_id', 'timestamp': 12345})

    # Open the key-value store by ID
    kvs2 = await KeyValueStore.open(
        id=kvs1.id,
        storage_backend=storage_backend,
    )

    # Verify it's the same key-value store
    assert kvs2.id == kvs1.id
    assert kvs2.name == 'kvs-by-id-test'

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


async def test_set_none_deletes_record(kvs: KeyValueStore) -> None:
    """Setting `None` deletes the record, so a later read falls back to the default value."""
    await kvs.set_value('none-key', 'value')

    await kvs.set_value('none-key', None)

    assert await kvs.record_exists('none-key') is False
    assert await kvs.get_value('none-key', 'default') == 'default'


@pytest.mark.parametrize(
    ('value', 'expected_value', 'expected_content_type'),
    [
        pytest.param(b'\x00\x01', b'\x00\x01', 'application/octet-stream', id='bytes'),
        pytest.param(bytearray(b'\x00\x01'), b'\x00\x01', 'application/octet-stream', id='bytearray'),
        pytest.param(memoryview(b'\x00\x01'), b'\x00\x01', 'application/octet-stream', id='memoryview'),
        pytest.param('text', 'text', 'text/plain; charset=utf-8', id='str'),
        pytest.param(42, 42, 'application/json; charset=utf-8', id='int'),
        pytest.param(1.5, 1.5, 'application/json; charset=utf-8', id='float'),
        pytest.param(True, True, 'application/json; charset=utf-8', id='bool'),
        pytest.param((1, 'two'), [1, 'two'], 'application/json; charset=utf-8', id='tuple'),
        pytest.param({'key': [1, 2]}, {'key': [1, 2]}, 'application/json; charset=utf-8', id='dict'),
    ],
)
async def test_set_value_infers_content_type(
    kvs: KeyValueStore, value: Any, expected_value: Any, expected_content_type: str
) -> None:
    """Without a content type, bytes are stored as binary data, `str` as text and any other value as JSON."""
    await kvs.set_value('key', value)

    record = await kvs.get_record('key')
    assert record is not None
    assert record.content_type == expected_content_type

    result = await kvs.get_value('key')
    assert result == expected_value
    assert type(result) is type(expected_value)


async def test_get_record_returns_raw_value(kvs: KeyValueStore) -> None:
    """`get_record` returns the stored bytes with their content type, and `None` for a missing record."""
    await kvs.set_value('key', 'příliš', content_type='text/html')

    record = await kvs.get_record('key')

    assert record is not None
    assert record.key == 'key'
    assert record.value == 'příliš'.encode()
    assert record.content_type == 'text/html'
    assert record.size == len(record.value)
    assert await kvs.get_record('nonexistent') is None


@pytest.mark.parametrize(
    'value',
    [
        pytest.param('{"key": [1, 2]}', id='str'),
        pytest.param(b'{"key": [1, 2]}', id='bytes'),
    ],
)
async def test_set_value_stores_serialized_json_as_is(kvs: KeyValueStore, value: str | bytes) -> None:
    """A `str` or bytes value with a JSON content type is stored as it is and read back parsed."""
    await kvs.set_value('key', value, content_type='application/json')

    record = await kvs.get_record('key')
    assert record is not None
    assert record.value == b'{"key": [1, 2]}'
    assert await kvs.get_value('key') == {'key': [1, 2]}


async def test_set_value_rejects_object_with_non_json_content_type(kvs: KeyValueStore) -> None:
    """A value that is neither `str` nor bytes can't be stored under a content type other than JSON."""
    with pytest.raises(TypeError, match='can only be stored as JSON'):
        await kvs.set_value('key', {'key': 'value'}, content_type='text/plain')

    assert await kvs.record_exists('key') is False


@pytest.mark.parametrize(
    ('content_type', 'text', 'encoding'),
    [
        pytest.param('text/html', '<p>žluťoučký</p>', 'utf-8', id='text'),
        pytest.param('application/xml', '<a>žluťoučký</a>', 'utf-8', id='xml'),
        pytest.param('application/rss+xml', '<rss>žluťoučký</rss>', 'utf-8', id='xml suffix'),
        pytest.param('text/plain; charset=windows-1250', 'žluťoučký', 'windows-1250', id='charset'),
    ],
)
async def test_text_values_are_read_as_str(kvs: KeyValueStore, content_type: str, text: str, encoding: str) -> None:
    """Text and XML values are stored and decoded to `str` with the charset of the content type."""
    await kvs.set_value('key', text, content_type=content_type)

    record = await kvs.get_record('key')
    assert record is not None
    assert record.value == text.encode(encoding)
    assert await kvs.get_value('key') == text


async def test_get_value_raises_on_unparsable_record(kvs: KeyValueStore) -> None:
    """A record that can't be parsed according to its content type raises and stays readable with `get_record`."""
    await kvs.set_value('key', b'{not json', content_type='application/json')

    with pytest.raises(json.JSONDecodeError) as exc_info:
        await kvs.get_value('key')

    assert any("key 'key'" in note for note in exc_info.value.__notes__)

    record = await kvs.get_record('key')
    assert record is not None
    assert record.value == b'{not json'


async def test_iterate_entries_raises_on_unparsable_record(kvs: KeyValueStore) -> None:
    """A record that can't be parsed according to its content type raises from `iterate_entries` too."""
    await kvs.set_value('key', b'{not json', content_type='application/json')

    with pytest.raises(json.JSONDecodeError) as exc_info:
        _ = [entry async for entry in kvs.iterate_entries()]

    assert any("key 'key'" in note for note in exc_info.value.__notes__)


async def test_set_value_rejects_unserializable_value(kvs: KeyValueStore) -> None:
    """A value JSON can't serialize raises and stores nothing."""
    value: dict[str, Any] = {}
    value['self'] = value

    with pytest.raises(ValueError, match='Circular reference'):
        await kvs.set_value('key', value)

    assert await kvs.record_exists('key') is False


@pytest.mark.parametrize(
    ('value', 'expected_value'),
    [
        pytest.param(
            {'created_at': datetime(2026, 1, 1, tzinfo=UTC)}, {'created_at': '2026-01-01T00:00:00Z'}, id='datetime'
        ),
        pytest.param(_Product(name='Phone', price=799.99), {'name': 'Phone', 'price': 799.99}, id='pydantic model'),
        pytest.param(
            _AliasedProduct.model_validate({'productName': 'Phone'}),
            {'productName': 'Phone'},
            id='pydantic model with an alias',
        ),
        pytest.param({'tags': {'a'}}, {'tags': ['a']}, id='set'),
        pytest.param({'body': b'text'}, {'body': 'text'}, id='bytes inside the value'),
        pytest.param({1: 'int key'}, {'1': 'int key'}, id='dict key that is not a string'),
    ],
)
async def test_set_value_converts_types_json_does_not_support(
    kvs: KeyValueStore, value: Any, expected_value: Any
) -> None:
    """Objects JSON doesn't support are stored the way Pydantic serializes them, and dict keys become strings."""
    await kvs.set_value('key', value)

    assert await kvs.get_value('key') == expected_value


@pytest.mark.parametrize(
    ('value', 'reason'),
    [
        pytest.param({'value': object()}, 'Unable to serialize unknown type', id='object of an unknown type'),
        pytest.param({'body': b'\xff'}, "can't decode byte", id='bytes that are not valid UTF-8'),
    ],
)
async def test_set_value_rejects_object_that_cannot_be_converted(kvs: KeyValueStore, value: Any, reason: str) -> None:
    """An object that Pydantic can't convert to JSON raises `TypeError` with the reason and stores nothing."""
    with pytest.raises(TypeError, match=reason):
        await kvs.set_value('key', value)

    assert await kvs.record_exists('key') is False


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


async def test_backend_page_carries_count_limit_and_start_key(kvs: KeyValueStore) -> None:
    """A backend page carries the number of its keys and the `limit` and `exclusive_start_key` it was requested with."""
    for key in ['a', 'b', 'c']:
        await kvs.set_value(key, 'value')

    page = await kvs._backend.list_keys(exclusive_start_key='a', limit=3)

    assert [item.key for item in page.items] == ['b', 'c']
    assert page.count == 2
    assert page.limit == 3
    assert page.exclusive_start_key == 'a'


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


async def test_iterate_values(kvs: KeyValueStore) -> None:
    """Test iterating over values in the key-value store."""
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', {'nested': 2})
    await kvs.set_value('key3', [3])

    collected_values = [value async for value in kvs.iterate_values()]

    assert len(collected_values) == 3
    assert 'value1' in collected_values
    assert {'nested': 2} in collected_values
    assert [3] in collected_values


async def test_iterate_entries(kvs: KeyValueStore) -> None:
    """Test iterating over (key, value) pairs in the key-value store."""
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', {'nested': 2})
    await kvs.set_value('key3', [3])

    collected_entries = dict([entry async for entry in kvs.iterate_entries()])

    assert collected_entries == {'key1': 'value1', 'key2': {'nested': 2}, 'key3': [3]}


async def test_iterate_entries_with_limit_and_exclusive_start_key(kvs: KeyValueStore) -> None:
    """Test that `iterate_entries` passes `limit` and `exclusive_start_key` through to the key listing."""
    for i in range(10):
        await kvs.set_value(f'key{i}', f'value{i}')

    all_keys = [metadata.key for metadata in await kvs.list_keys()]
    start_key = all_keys[2]

    collected_entries = [entry async for entry in kvs.iterate_entries(exclusive_start_key=start_key, limit=3)]

    assert len(collected_entries) == 3
    expected_keys = all_keys[all_keys.index(start_key) + 1 :][:3]
    assert [key for key, _ in collected_entries] == expected_keys
    assert all(value == f'value{key.removeprefix("key")}' for key, value in collected_entries)


async def test_iterate_entries_empty_kvs(kvs: KeyValueStore) -> None:
    """Test that iterating over an empty key-value store yields nothing."""
    collected_entries = [entry async for entry in kvs.iterate_entries()]

    assert collected_entries == []


async def test_iterate_entries_reads_values_in_batches(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """The values are read with `get_values` in batches bounded by the number of keys."""
    for i in range(5):
        await kvs.set_value(f'key{i}', f'value{i}')

    get_values = AsyncMock(wraps=kvs._backend.get_values)
    monkeypatch.setattr(kvs._backend, 'get_values', get_values)
    monkeypatch.setattr(KeyValueStore, '_ITERATE_ENTRIES_BATCH_MAX_KEYS', 2)

    entries = [entry async for entry in kvs.iterate_entries()]

    assert entries == [(f'key{i}', f'value{i}') for i in range(5)]
    assert [call.kwargs['keys'] for call in get_values.await_args_list] == [
        ['key0', 'key1'],
        ['key2', 'key3'],
        ['key4'],
    ]


async def test_iterate_entries_batches_are_bounded_by_size(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """The batches are bounded by the record sizes, and a record over the limit is read alone in its batch."""
    await kvs.set_value('small1', 'ab')
    await kvs.set_value('small2', 'cd')
    await kvs.set_value('large', 'x' * 100)
    await kvs.set_value('small3', 'ef')

    get_values = AsyncMock(wraps=kvs._backend.get_values)
    monkeypatch.setattr(kvs._backend, 'get_values', get_values)
    monkeypatch.setattr(KeyValueStore, '_ITERATE_ENTRIES_BATCH_MAX_BYTES', 10)

    keys = [key async for key, _ in kvs.iterate_entries()]

    assert keys == ['large', 'small1', 'small2', 'small3']
    assert [call.kwargs['keys'] for call in get_values.await_args_list] == [['large'], ['small1', 'small2', 'small3']]


async def test_iterate_entries_follows_key_order(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """The entries follow the order of the keys, whatever order the storage backend returns the records in."""
    for key in ['a', 'b', 'c']:
        await kvs.set_value(key, f'value-{key}')

    records_in_reverse_order = {key: await kvs.get_record(key) for key in ['c', 'b', 'a']}
    monkeypatch.setattr(kvs._backend, 'get_values', AsyncMock(return_value=records_in_reverse_order))

    entries = [entry async for entry in kvs.iterate_entries()]

    assert entries == [('a', 'value-a'), ('b', 'value-b'), ('c', 'value-c')]


async def test_iterate_entries_skips_record_deleted_after_listing(
    kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A record deleted after its key was listed, but before its value was read, is skipped."""
    for key in ['a', 'b', 'c']:
        await kvs.set_value(key, f'value-{key}')

    monkeypatch.setattr(KeyValueStore, '_ITERATE_ENTRIES_BATCH_MAX_KEYS', 1)

    entries = []
    async for key, value in kvs.iterate_entries():
        if key == 'a':
            await kvs.delete_value('b')
        entries.append((key, value))

    assert entries == [('a', 'value-a'), ('c', 'value-c')]


@pytest.mark.parametrize(
    ('limit', 'expected_count'),
    [
        pytest.param(None, 10, id='no limit'),
        pytest.param(7, 7, id='limit inside a later page'),
        pytest.param(3, 3, id='limit at a page boundary'),
    ],
)
async def test_iteration_spans_multiple_pages(kvs: KeyValueStore, limit: int | None, expected_count: int) -> None:
    """The keys and the entries are listed page by page, up to the limit, without skipping or repeating a key."""
    for i in range(10):
        await kvs.set_value(f'key{i}', i)

    keys = [metadata.key async for metadata in kvs.iterate_keys(limit=limit, chunk_size=3)]
    entries = [entry async for entry in kvs.iterate_entries(limit=limit, chunk_size=3)]

    assert keys == [f'key{i}' for i in range(expected_count)]
    assert entries == [(f'key{i}', i) for i in range(expected_count)]


async def test_iteration_continues_past_short_pages(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """A short or empty page before the last one doesn't end the iteration, and only its keys count toward the limit."""
    pages = [
        KeyValueStoreKeysListPage(
            items=[KeyValueStoreRecordMetadata(key='a', content_type='text/plain')],
            count=1,
            limit=3,
            next_exclusive_start_key='b',
        ),
        KeyValueStoreKeysListPage(items=[], count=0, limit=2, exclusive_start_key='b', next_exclusive_start_key='c'),
        KeyValueStoreKeysListPage(
            items=[KeyValueStoreRecordMetadata(key=key, content_type='text/plain') for key in ['d', 'e']],
            count=2,
            limit=2,
            exclusive_start_key='c',
            next_exclusive_start_key='e',
        ),
    ]
    list_keys = AsyncMock(side_effect=pages)
    monkeypatch.setattr(kvs._backend, 'list_keys', list_keys)

    keys = [metadata.key async for metadata in kvs.iterate_keys(limit=3)]

    assert keys == ['a', 'd', 'e']
    assert [call.kwargs['limit'] for call in list_keys.await_args_list] == [3, 2, 2]


async def test_iteration_with_prefix(kvs: KeyValueStore) -> None:
    """Only the records whose key starts with the prefix are listed and iterated."""
    await kvs.set_value('Item-1', 'a')
    await kvs.set_value('Item-2', 'b')
    await kvs.set_value('item-3', 'c')
    await kvs.set_value('other', 'd')

    listed_keys = [metadata.key for metadata in await kvs.list_keys(prefix='Item-')]
    values = [value async for value in kvs.iterate_values(prefix='Item-')]

    assert listed_keys == ['Item-1', 'Item-2']
    assert values == ['a', 'b']


async def test_iteration_with_prefix_spans_multiple_pages(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """The keys with the prefix are listed page by page, and the page with the last of them ends the listing."""
    for key in ['a', 'item-1', 'item-2', 'item-3', 'item-4', 'j']:
        await kvs.set_value(key, 'value')

    list_keys = AsyncMock(wraps=kvs._backend.list_keys)
    monkeypatch.setattr(kvs._backend, 'list_keys', list_keys)

    keys = [metadata.key async for metadata in kvs.iterate_keys(prefix='item-', chunk_size=2)]

    assert keys == ['item-1', 'item-2', 'item-3', 'item-4']
    assert list_keys.await_count == 2


@pytest.mark.parametrize(
    ('chunk_size', 'expected_limits'),
    [
        pytest.param(4, [4, 4, 4], id='three pages'),
        pytest.param(100, [100], id='one page for the whole store'),
    ],
)
async def test_chunk_size_sets_keys_per_backend_call(
    kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch, chunk_size: int, expected_limits: list[int]
) -> None:
    """`chunk_size` sets how many keys each storage backend call lists."""
    for i in range(10):
        await kvs.set_value(f'key{i}', i)

    list_keys = AsyncMock(wraps=kvs._backend.list_keys)
    monkeypatch.setattr(kvs._backend, 'list_keys', list_keys)

    keys = [metadata.key async for metadata in kvs.iterate_keys(chunk_size=chunk_size)]

    assert keys == [f'key{i}' for i in range(10)]
    assert [call.kwargs['limit'] for call in list_keys.await_args_list] == expected_limits


@pytest.mark.parametrize(
    ('chunk_size', 'expected_keys'),
    [
        pytest.param(1, ['a', 'a-done', 'b'], id='pages smaller than the store'),
        pytest.param(10, ['a', 'b'], id='one page for the whole store'),
    ],
)
async def test_keys_set_during_iteration(kvs: KeyValueStore, chunk_size: int, expected_keys: list[str]) -> None:
    """A key set during the iteration shows up in it, unless all keys were listed with the first page."""
    for key in ['a', 'b']:
        await kvs.set_value(key, 'value')

    listed_keys = list[str]()
    async for metadata in kvs.iterate_keys(chunk_size=chunk_size):
        listed_keys.append(metadata.key)
        if not metadata.key.endswith('-done'):
            await kvs.set_value(f'{metadata.key}-done', 'value')

    assert listed_keys == expected_keys


async def test_chunk_size_below_one_raises(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """A `chunk_size` below 1 raises before the storage backend is called."""
    list_keys = AsyncMock(wraps=kvs._backend.list_keys)
    monkeypatch.setattr(kvs._backend, 'list_keys', list_keys)

    with pytest.raises(ValueError, match='at least 1'):
        _ = [metadata async for metadata in kvs.iterate_keys(chunk_size=0)]

    list_keys.assert_not_awaited()


@pytest.mark.parametrize(
    ('exclusive_start_key', 'expected_keys'),
    [
        pytest.param('Z', ['item-1', 'item-2'], id='start key before the prefix'),
        pytest.param('item-1', ['item-2'], id='start key with the prefix'),
        pytest.param('z', [], id='start key after the prefix'),
    ],
)
async def test_list_keys_with_prefix_and_exclusive_start_key(
    kvs: KeyValueStore, exclusive_start_key: str, expected_keys: list[str]
) -> None:
    """The start key narrows the keys with the prefix, wherever it sorts relative to the prefix."""
    for key in ['a', 'item-1', 'item-2', 'j']:
        await kvs.set_value(key, 'value')

    listed_keys = await kvs.list_keys(prefix='item-', exclusive_start_key=exclusive_start_key)

    assert [metadata.key for metadata in listed_keys] == expected_keys


async def test_async_iteration(kvs: KeyValueStore) -> None:
    """Test that the key-value store can be used directly in an `async for` loop, yielding keys like a dict."""
    await kvs.set_value('key1', 'value1')
    await kvs.set_value('key2', 'value2')

    collected_keys = [key async for key in kvs]

    assert sorted(collected_keys) == ['key1', 'key2']


async def test_drop(
    storage_backend: StorageBackend,
) -> None:
    """Test dropping a key-value store removes it from cache and clears its data."""
    kvs = await KeyValueStore.open(
        name='drop-test',
        storage_backend=storage_backend,
    )

    # Add some data
    await kvs.set_value('test', 'data')

    # Drop the key-value store
    await kvs.drop()

    # Verify key-value store is empty (by creating a new one with the same name)
    new_kvs = await KeyValueStore.open(
        name='drop-test',
        storage_backend=storage_backend,
    )

    # Attempt to get a previously stored value
    result = await new_kvs.get_value('test')
    assert result is None
    await new_kvs.drop()


async def test_reopen_default(
    storage_backend: StorageBackend,
) -> None:
    """Test reopening the default key-value store."""
    # Open the default key-value store
    kvs1 = await KeyValueStore.open(
        storage_backend=storage_backend,
    )

    # Set a value
    await kvs1.set_value('test_key', 'test_value')

    # Open the default key-value store again
    kvs2 = await KeyValueStore.open(
        storage_backend=storage_backend,
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


async def test_data_persistence_on_reopen() -> None:
    """Test that data persists when reopening a KeyValueStore."""
    kvs1 = await KeyValueStore.open()

    await kvs1.set_value('key_123', 'value_123')

    result1 = await kvs1.get_value('key_123')
    assert result1 == 'value_123'

    kvs2 = await KeyValueStore.open()

    result2 = await kvs2.get_value('key_123')
    assert result2 == 'value_123'
    assert await kvs1.list_keys() == await kvs2.list_keys()

    await kvs2.set_value('key_456', 'value_456')

    result1 = await kvs1.get_value('key_456')
    assert result1 == 'value_456'


async def test_purge(
    storage_backend: StorageBackend,
) -> None:
    """Test purging a key-value store removes all values but keeps the store itself."""
    # First create a key-value store
    kvs = await KeyValueStore.open(
        name='purge-test-kvs',
        storage_backend=storage_backend,
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
    assert kvs.name == 'purge-test-kvs'  # Same name preserved

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
        ('key4', 42),
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


async def test_open_with_alias(
    storage_backend: StorageBackend,
) -> None:
    """Test opening key-value stores with alias parameter for NDU functionality."""
    # Create key-value stores with different aliases
    kvs_1 = await KeyValueStore.open(
        alias='test_alias_1',
        storage_backend=storage_backend,
    )
    kvs_2 = await KeyValueStore.open(
        alias='test_alias_2',
        storage_backend=storage_backend,
    )

    # Verify they have different IDs but no names (unnamed)
    assert kvs_1.id != kvs_2.id
    assert kvs_1.name is None
    assert kvs_2.name is None

    # Add different data to each
    await kvs_1.set_value('source', 'alias_1')
    await kvs_2.set_value('source', 'alias_2')

    # Verify data isolation
    value_1 = await kvs_1.get_value('source')
    value_2 = await kvs_2.get_value('source')

    assert value_1 == 'alias_1'
    assert value_2 == 'alias_2'

    # Clean up
    await kvs_1.drop()
    await kvs_2.drop()


async def test_alias_caching(
    storage_backend: StorageBackend,
) -> None:
    """Test that key-value stores with same alias return same instance (cached)."""
    # Open kvs with alias
    kvs_1 = await KeyValueStore.open(
        alias='cache_test',
        storage_backend=storage_backend,
    )

    # Open again with same alias
    kvs_2 = await KeyValueStore.open(
        alias='cache_test',
        storage_backend=storage_backend,
    )

    # Should be same instance
    assert kvs_1 is kvs_2
    assert kvs_1.id == kvs_2.id

    # Clean up
    await kvs_1.drop()


async def test_alias_with_id_error(
    storage_backend: StorageBackend,
) -> None:
    """Test that providing both alias and id raises error."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "id", "alias".',
    ):
        await KeyValueStore.open(
            id='some-id',
            alias='some-alias',
            storage_backend=storage_backend,
        )


async def test_alias_with_name_error(
    storage_backend: StorageBackend,
) -> None:
    """Test that providing both alias and name raises error."""
    with pytest.raises(
        ValueError,
        match=r'Only one of "id", "name", "alias" can be specified, but following arguments '
        r'were specified: "name", "alias".',
    ):
        await KeyValueStore.open(
            name='some-name',
            alias='some-alias',
            storage_backend=storage_backend,
        )


async def test_alias_with_special_characters(
    storage_backend: StorageBackend,
) -> None:
    """Test alias functionality with special characters."""
    special_aliases = [
        'alias-with-dashes',
        'alias_with_underscores',
        'alias.with.dots',
        'alias123with456numbers',
        'CamelCaseAlias',
    ]

    stores = []
    for alias in special_aliases:
        kvs = await KeyValueStore.open(
            alias=alias,
            storage_backend=storage_backend,
        )
        stores.append(kvs)

        # Add data with the alias as identifier
        await kvs.set_value('alias_used', alias)
        await kvs.set_value('test', 'special_chars')

    # Verify all work correctly
    for i, kvs in enumerate(stores):
        assert await kvs.get_value('alias_used') == special_aliases[i]
        assert await kvs.get_value('test') == 'special_chars'

    # Clean up
    for kvs in stores:
        await kvs.drop()


async def test_alias_key_operations(
    storage_backend: StorageBackend,
) -> None:
    """Test that key operations work correctly with alias stores."""
    kvs = await KeyValueStore.open(
        alias='key_ops_test',
        storage_backend=storage_backend,
    )

    # Test setting multiple keys
    test_data = {
        'key1': {'data': 'value1', 'number': 1},
        'key2': 'simple string value',
        'key3': [1, 2, 3, 4, 5],
        'key4': 42,
    }

    for key, value in test_data.items():
        await kvs.set_value(key, value)

    # Test getting all keys
    keys = await kvs.list_keys()
    key_names = [k.key for k in keys]
    assert len(keys) == 4
    for key in test_data:
        assert key in key_names

    # Test record_exists
    for key in test_data:
        assert await kvs.record_exists(key) is True
    assert await kvs.record_exists('nonexistent') is False

    # Test iteration
    collected_keys = [key async for key in kvs.iterate_keys()]
    assert len(collected_keys) == 4

    # Test deletion
    await kvs.delete_value('key2')
    assert await kvs.record_exists('key2') is False
    assert await kvs.get_value('key2') is None

    # Verify other keys still exist
    remaining_keys = await kvs.list_keys()
    assert len(remaining_keys) == 3

    # Clean up
    await kvs.drop()


async def test_named_vs_alias_conflict_detection(
    storage_backend: StorageBackend,
) -> None:
    """Test that conflicts between named and alias storages are detected."""
    # Test 1: Create named storage first, then try alias with same name
    named_kvs = await KeyValueStore.open(name='conflict-test', storage_backend=storage_backend)
    assert named_kvs.name == 'conflict-test'

    # Try to create alias with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create alias storage "conflict-test".*already exists'):
        await KeyValueStore.open(alias='conflict-test', storage_backend=storage_backend)

    # Clean up
    await named_kvs.drop()

    # Test 2: Create alias first, then try named with same name
    alias_kvs = await KeyValueStore.open(alias='conflict-test2', storage_backend=storage_backend)
    assert alias_kvs.name is None  # Alias storages have no name

    # Try to create named with same name - should raise error
    with pytest.raises(ValueError, match=r'Cannot create named storage "conflict-test2".*already exists'):
        await KeyValueStore.open(name='conflict-test2', storage_backend=storage_backend)

    # Clean up
    await alias_kvs.drop()

    # Test 3: Different names should work fine
    named_kvs_ok = await KeyValueStore.open(name='different-name', storage_backend=storage_backend)
    alias_kvs_ok = await KeyValueStore.open(alias='different-alias', storage_backend=storage_backend)

    assert named_kvs_ok.name == 'different-name'
    assert alias_kvs_ok.name is None

    # Clean up
    await named_kvs_ok.drop()
    await alias_kvs_ok.drop()


async def test_alias_parameter(
    storage_backend: StorageBackend,
) -> None:
    """Test key-value store creation and operations with alias parameter."""
    # Create kvs with alias
    alias_kvs = await KeyValueStore.open(
        alias='test_alias',
        storage_backend=storage_backend,
    )

    # Verify alias kvs properties
    assert alias_kvs.id is not None
    assert alias_kvs.name is None  # Alias storages should be unnamed

    # Test data operations
    await alias_kvs.set_value('test_key', {'type': 'alias', 'value': 1})
    value = await alias_kvs.get_value('test_key')
    assert value['type'] == 'alias'

    await alias_kvs.drop()


async def test_alias_vs_named_isolation(
    storage_backend: StorageBackend,
) -> None:
    """Test that alias and named key-value stores with same identifier are isolated."""
    # Create named kvs
    named_kvs = await KeyValueStore.open(
        name='test-identifier',
        storage_backend=storage_backend,
    )

    # Verify named kvs
    assert named_kvs.name == 'test-identifier'
    await named_kvs.set_value('type', 'named')

    # Clean up named kvs first
    await named_kvs.drop()

    # Now create alias kvs with same identifier (should work after cleanup)
    alias_kvs = await KeyValueStore.open(
        alias='test_identifier',
        storage_backend=storage_backend,
    )

    # Should be different instance
    assert alias_kvs.name is None
    await alias_kvs.set_value('type', 'alias')

    # Verify alias data
    alias_value = await alias_kvs.get_value('type')
    assert alias_value == 'alias'

    await alias_kvs.drop()


async def test_default_vs_alias_default_equivalence(
    storage_backend: StorageBackend,
) -> None:
    """Test that default key-value store and alias='default' are equivalent."""
    # Open default kvs
    default_kvs = await KeyValueStore.open(
        storage_backend=storage_backend,
    )

    alias_default_kvs = await KeyValueStore.open(
        alias=StorageInstanceManager._DEFAULT_STORAGE_ALIAS,
        storage_backend=storage_backend,
    )

    # Should be the same
    assert default_kvs.id == alias_default_kvs.id
    assert default_kvs.name is None
    assert alias_default_kvs.name is None

    # Data should be shared
    await default_kvs.set_value('source', 'default')
    value = await alias_default_kvs.get_value('source')
    assert value == 'default'

    await default_kvs.drop()


async def test_multiple_alias_isolation(
    storage_backend: StorageBackend,
) -> None:
    """Test that different aliases create separate key-value stores."""
    kvs_stores = []

    for i in range(3):
        kvs = await KeyValueStore.open(
            alias=f'alias_{i}',
            storage_backend=storage_backend,
        )
        await kvs.set_value('alias', f'alias_{i}')
        await kvs.set_value('index', i)
        kvs_stores.append(kvs)

    # All should be different
    for i in range(3):
        for j in range(i + 1, 3):
            assert kvs_stores[i].id != kvs_stores[j].id

    # Verify data isolation
    for i, kvs in enumerate(kvs_stores):
        alias_value = await kvs.get_value('alias')
        index_value = await kvs.get_value('index')
        assert alias_value == f'alias_{i}'
        # For memory storage, value is preserved as int; for filesystem it's converted to string
        assert index_value == i or index_value == str(i)
        await kvs.drop()


async def test_purge_on_start_enabled(storage_backend: StorageBackend) -> None:
    """Test purge behavior when purge_on_start=True: named storages retain data, unnamed storages are purged."""

    # Skip this test for memory storage since it doesn't persist data between backend instances.
    if isinstance(storage_backend, MemoryStorageBackend):
        raise pytest.skip.Exception('Memory storage does not persist data between backend instances.')

    configuration = Configuration(purge_on_start=True)

    # First, create all storage types with purge enabled and add data.
    default_kvs = await KeyValueStore.open(
        storage_backend=storage_backend,
        configuration=configuration,
    )

    alias_kvs = await KeyValueStore.open(
        alias='purge-test-alias',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    named_kvs = await KeyValueStore.open(
        name='purge-test-named',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    await default_kvs.set_value(key='data', value='should_be_purged')
    await alias_kvs.set_value(key='data', value='should_be_purged')
    await named_kvs.set_value(key='data', value='should_persist')

    # Verify data was added
    default_data = await default_kvs.get_value(key='data')
    alias_data = await alias_kvs.get_value(key='data')
    named_data = await named_kvs.get_value(key='data')

    assert default_data == 'should_be_purged'
    assert alias_data == 'should_be_purged'
    assert named_data == 'should_persist'

    # Verify that default and alias storages are unnamed
    default_metadata = await default_kvs.get_metadata()
    alias_metadata = await alias_kvs.get_metadata()
    named_metadata = await named_kvs.get_metadata()

    assert default_metadata.name is None
    assert alias_metadata.name is None
    assert named_metadata.name == 'purge-test-named'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_kvs_2 = await KeyValueStore.open(
        storage_backend=storage_backend,
        configuration=configuration,
    )
    alias_kvs_2 = await KeyValueStore.open(
        alias='purge-test-alias',
        storage_backend=storage_backend,
        configuration=configuration,
    )
    named_kvs_2 = await KeyValueStore.open(
        name='purge-test-named',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    # Check the data after purge
    default_data_after = await default_kvs_2.get_value(key='data')
    alias_data_after = await alias_kvs_2.get_value(key='data')
    named_data_after = await named_kvs_2.get_value(key='data')

    # Unnamed storages (alias and default) should be purged (data removed)
    assert default_data_after is None
    assert alias_data_after is None

    # Named storage should retain data (not purged)
    assert named_data_after == 'should_persist'

    # Clean up
    await named_kvs_2.drop()
    await alias_kvs_2.drop()
    await default_kvs_2.drop()


async def test_purge_on_start_disabled(storage_backend: StorageBackend) -> None:
    """Test purge behavior when purge_on_start=False: all storages retain data regardless of type."""

    # Skip this test for memory storage since it doesn't persist data between backend instances.
    if isinstance(storage_backend, MemoryStorageBackend):
        raise pytest.skip.Exception('Memory storage does not persist data between backend instances.')

    configuration = Configuration(purge_on_start=False)

    # First, create all storage types with purge disabled and add data.
    default_kvs = await KeyValueStore.open(
        storage_backend=storage_backend,
        configuration=configuration,
    )

    alias_kvs = await KeyValueStore.open(
        alias='purge-test-alias',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    named_kvs = await KeyValueStore.open(
        name='purge-test-named',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    await default_kvs.set_value('data', 'should_persist')
    await alias_kvs.set_value('data', 'should_persist')
    await named_kvs.set_value('data', 'should_persist')

    # Verify data was added
    default_data = await default_kvs.get_value('data')
    alias_data = await alias_kvs.get_value('data')
    named_data = await named_kvs.get_value('data')

    assert default_data == 'should_persist'
    assert alias_data == 'should_persist'
    assert named_data == 'should_persist'

    # Clear storage cache to simulate "reopening" storages
    service_locator.storage_instance_manager.clear_cache()

    # Now "reopen" all storages
    default_kvs_2 = await KeyValueStore.open(
        storage_backend=storage_backend,
        configuration=configuration,
    )
    alias_kvs_2 = await KeyValueStore.open(
        alias='purge-test-alias',
        storage_backend=storage_backend,
        configuration=configuration,
    )
    named_kvs_2 = await KeyValueStore.open(
        name='purge-test-named',
        storage_backend=storage_backend,
        configuration=configuration,
    )

    # Check the data after reopen
    default_data_after = await default_kvs_2.get_value('data')
    alias_data_after = await alias_kvs_2.get_value('data')
    named_data_after = await named_kvs_2.get_value('data')

    # All storages should retain data when purge is disabled
    assert default_data_after == 'should_persist'
    assert alias_data_after == 'should_persist'
    assert named_data_after == 'should_persist'

    # Clean up
    await named_kvs_2.drop()
    await alias_kvs_2.drop()
    await default_kvs_2.drop()


async def test_name_default_not_allowed(storage_backend: StorageBackend) -> None:
    """Test that storage can't have default alias as name, to prevent collisions with unnamed storage alias."""
    with pytest.raises(
        ValueError,
        match=f'Storage name cannot be "{StorageInstanceManager._DEFAULT_STORAGE_ALIAS}" as '
        f'it is reserved for default alias.',
    ):
        await KeyValueStore.open(name=StorageInstanceManager._DEFAULT_STORAGE_ALIAS, storage_backend=storage_backend)


@pytest.mark.parametrize(
    ('name', 'is_valid'),
    [
        pytest.param('F', True, id='single-char'),
        pytest.param('7', True, id='single-digit'),
        pytest.param('FtghdfseySds', True, id='mixed-case'),
        pytest.param('125673450', True, id='all-digits'),
        pytest.param('Ft2134Sfe0O1hf', True, id='mixed-alphanumeric'),
        pytest.param('name-with-dashes', True, id='dashes'),
        pytest.param('1-value', True, id='number start'),
        pytest.param('value-1', True, id='number end'),
        pytest.param('test-1-value', True, id='number middle'),
        pytest.param('test-------value', True, id='multiple-dashes'),
        pytest.param('test-VALUES-test', True, id='multiple-cases'),
        pytest.param('name_with_underscores', False, id='underscores'),
        pytest.param('name with spaces', False, id='spaces'),
        pytest.param('-test', False, id='dashes start'),
        pytest.param('test-', False, id='dashes end'),
        pytest.param('../outside', False, id='parent-ref'),
        pytest.param('with/slash', False, id='slash'),
    ],
)
async def test_validate_name(storage_backend: StorageBackend, name: str, *, is_valid: bool) -> None:
    """Test name validation logic."""
    if is_valid:
        # Should not raise
        dataset = await KeyValueStore.open(name=name, storage_backend=storage_backend)
        assert dataset.name == name
        await dataset.drop()
    else:
        with pytest.raises(ValueError, match=rf'Invalid storage name "{name}".*'):
            await KeyValueStore.open(name=name, storage_backend=storage_backend)


@pytest.mark.parametrize(
    ('alias', 'is_valid'),
    [
        pytest.param('valid-alias', True, id='dashes'),
        pytest.param('alias_with_underscores', True, id='underscores'),
        pytest.param('alias.with.dots', True, id='dots'),
        pytest.param('CamelCaseAlias', True, id='mixed-case'),
        pytest.param('../outside', False, id='parent-ref'),
        pytest.param('..', False, id='bare-parent'),
        pytest.param('.', False, id='bare-current'),
        pytest.param('nested/alias', False, id='slash'),
        pytest.param('back\\slash', False, id='backslash'),
    ],
)
async def test_validate_alias(storage_backend: StorageBackend, alias: str, *, is_valid: bool) -> None:
    """Test alias validation logic, including rejection of values that would resolve outside the storage dir."""
    if is_valid:
        # Should not raise.
        kvs = await KeyValueStore.open(alias=alias, storage_backend=storage_backend)
        await kvs.drop()
    else:
        with pytest.raises(ValueError, match=r'Invalid storage alias'):
            await KeyValueStore.open(alias=alias, storage_backend=storage_backend)


@pytest.mark.parametrize(
    'tested_storage_backend_class',
    [
        pytest.param(MemoryStorageBackend, id='tested=MemoryStorageBackend'),
        pytest.param(FileSystemStorageBackend, id='tested=FileSystemStorageBackend'),
        pytest.param(SqlStorageBackend, id='tested=SqlStorageBackend'),
    ],
)
@pytest.mark.parametrize(
    'global_storage_backend_class',
    [
        pytest.param(MemoryStorageBackend, id='global=MemoryStorageBackend'),
        pytest.param(FileSystemStorageBackend, id='global=FileSystemStorageBackend'),
        pytest.param(SqlStorageBackend, id='global=SqlStorageBackend'),
    ],
)
async def test_get_auto_saved_value_various_global_backends(
    tmp_path: Path,
    tested_storage_backend_class: type[StorageBackend],
    global_storage_backend_class: type[StorageBackend],
) -> None:
    """Ensure that persistence is working for all backends regardless of what is set in service locator."""
    tested_storage_backend = tested_storage_backend_class()
    global_storage_backend = global_storage_backend_class()

    service_locator.set_configuration(
        Configuration(
            storage_dir=str(tmp_path),
            purge_on_start=True,
        )
    )
    service_locator.set_storage_backend(global_storage_backend)

    kvs = await KeyValueStore.open(storage_backend=tested_storage_backend)
    values_kvs = {'key': 'some_value'}
    test_key = 'test_key'

    autosaved_value_kvs = await kvs.get_auto_saved_value(test_key)
    assert autosaved_value_kvs == {}
    autosaved_value_kvs.update(values_kvs)
    await kvs.persist_autosaved_values()

    assert await kvs.get_value(test_key) == autosaved_value_kvs


async def test_record_with_noascii_chars(kvs: KeyValueStore) -> None:
    """Test storing and retrieving a record with non-ASCII characters."""
    init_value = {
        'record_1': 'Supermaxi El Jardín',
        'record_2': 'záznam dva',
        'record_3': '記録三',
    }
    key = 'non_ascii_key'

    # Save the record in the key-value store
    await kvs.set_value(key, init_value)

    # Get the record and verify
    value = await kvs.get_value(key)
    assert value is not None
    assert value == init_value


async def test_persist_autosaved_values_with_concurrent_new_key(
    kvs: KeyValueStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test persisting autosaved values while adding a new autosaved key."""
    await kvs.get_auto_saved_value('existing')

    # Park the persist loop mid-iteration by blocking the underlying write, then insert a new key.
    persist_entered = asyncio.Event()
    release_persist = asyncio.Event()
    original_set_value = kvs.set_value

    async def blocking_set_value(*args: Any, **kwargs: Any) -> None:
        persist_entered.set()
        await asyncio.wait_for(release_persist.wait(), timeout=5)
        await original_set_value(*args, **kwargs)

    monkeypatch.setattr(kvs, 'set_value', blocking_set_value)

    persist_task = asyncio.create_task(kvs.persist_autosaved_values())
    await asyncio.wait_for(persist_entered.wait(), timeout=5)

    insert_started = asyncio.Event()

    async def insert_autosaved_value() -> None:
        insert_started.set()
        await kvs.get_auto_saved_value('added-during-persist')

    insert_task = asyncio.create_task(insert_autosaved_value())
    await asyncio.wait_for(insert_started.wait(), timeout=5)
    release_persist.set()

    # Should not raise `RuntimeError: dictionary changed size during iteration`.
    await asyncio.wait_for(asyncio.gather(persist_task, insert_task), timeout=5)


async def test_purge_with_concurrent_new_autosaved_key(
    kvs: KeyValueStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An autosaved key added during a purge stays cached after it."""
    await kvs.get_auto_saved_value('existing')

    # Park the purge in the last save of the existing value by blocking the underlying write, then insert a new key.
    purge_entered = asyncio.Event()
    release_purge = asyncio.Event()
    original_set_value = kvs.set_value

    async def blocking_set_value(*args: Any, **kwargs: Any) -> None:
        purge_entered.set()
        await asyncio.wait_for(release_purge.wait(), timeout=5)
        await original_set_value(*args, **kwargs)

    monkeypatch.setattr(kvs, 'set_value', blocking_set_value)

    purge_task = asyncio.create_task(kvs.purge())
    await asyncio.wait_for(purge_entered.wait(), timeout=5)

    insert_started = asyncio.Event()

    async def insert_autosaved_value() -> Any:
        insert_started.set()
        return await kvs.get_auto_saved_value('added-during-purge')

    insert_task = asyncio.create_task(insert_autosaved_value())
    await asyncio.wait_for(insert_started.wait(), timeout=5)
    release_purge.set()

    _, added_value = await asyncio.wait_for(asyncio.gather(purge_task, insert_task), timeout=5)

    assert await kvs.get_auto_saved_value('added-during-purge') is added_value


async def test_get_auto_saved_value_retries_cancelled_load(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """After a call cancelled during the load, the next `get_auto_saved_value` returns the stored record."""
    await kvs.set_value('state', {'counter': 42})
    get_value = kvs.get_value
    load_started = asyncio.Event()

    async def hanging_get_value(*_args: Any, **_kwargs: Any) -> None:
        load_started.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=hanging_get_value))
    load_task = asyncio.create_task(kvs.get_auto_saved_value('state'))
    await load_started.wait()
    load_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await load_task

    monkeypatch.setattr(kvs, 'get_value', get_value)

    assert await kvs.get_auto_saved_value('state') == {'counter': 42}


async def test_get_auto_saved_value_keeps_invalid_record(kvs: KeyValueStore) -> None:
    """A stored value that isn't a mapping makes `get_auto_saved_value` raise and isn't overwritten."""
    await kvs.set_value('state', [1, 2, 3])

    with pytest.raises(ValidationError):
        await kvs.get_auto_saved_value('state')

    await kvs.persist_autosaved_values()

    assert await kvs.get_value('state') == [1, 2, 3]
    with pytest.raises(ValidationError):
        await kvs.get_auto_saved_value('state')


async def test_set_value_stores_snapshot(kvs: KeyValueStore) -> None:
    """Mutating a value after setting it doesn't change the stored value."""
    value: dict[str, Any] = {'key': 'value', 'nested': {'list': [1, 2]}}
    await kvs.set_value('test-key', value)

    value['key'] = 'changed'
    value['nested']['list'].append(3)

    assert await kvs.get_value('test-key') == {'key': 'value', 'nested': {'list': [1, 2]}}


async def test_mutating_read_value_does_not_change_stored_value(kvs: KeyValueStore) -> None:
    """Mutating a value returned by `get_value` doesn't change the stored value."""
    await kvs.set_value('test-key', {'key': 'value'})

    value = await kvs.get_value('test-key')
    value['key'] = 'changed'

    assert await kvs.get_value('test-key') == {'key': 'value'}


async def _emit_persist_state() -> None:
    event_manager = service_locator.get_event_manager()
    async with event_manager:
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()


async def test_purge_drops_autosaved_values(kvs: KeyValueStore) -> None:
    """A purge drops the autosaved values, so neither the next save nor the next read brings them back."""
    autosaved_value = await kvs.get_auto_saved_value('state', {'count': 0})
    autosaved_value['count'] = 1

    await kvs.purge()
    autosaved_value['count'] = 2
    await kvs.persist_autosaved_values()
    await _emit_persist_state()

    assert await kvs.get_value('state') is None
    assert await kvs.get_auto_saved_value('state', {'count': 0}) == {'count': 0}


async def test_drop_drops_autosaved_values(storage_backend: StorageBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    """A drop drops the autosaved values, so no later save writes them to the dropped store."""
    kvs = await KeyValueStore.open(name='drop-test', storage_backend=storage_backend)
    autosaved_value = await kvs.get_auto_saved_value('state', {'count': 0})
    await kvs.drop()

    set_value = AsyncMock()
    monkeypatch.setattr(kvs, 'set_value', set_value)
    autosaved_value['count'] = 1
    await kvs.persist_autosaved_values()
    await _emit_persist_state()

    set_value.assert_not_called()


async def test_purge_with_concurrent_get_auto_saved_value(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """A `get_auto_saved_value` call made during a purge doesn't load the value that is being purged."""
    autosaved_value = await kvs.get_auto_saved_value('state', {'count': 0})
    autosaved_value['count'] = 1

    purge_started = asyncio.Event()
    release_purge = asyncio.Event()
    backend_purge = kvs._backend.purge

    async def blocking_purge() -> None:
        purge_started.set()
        await asyncio.wait_for(release_purge.wait(), timeout=5)
        await backend_purge()

    monkeypatch.setattr(kvs._backend, 'purge', AsyncMock(side_effect=blocking_purge))

    purge_task = asyncio.create_task(kvs.purge())
    await asyncio.wait_for(purge_started.wait(), timeout=5)

    # The read gets time to load the record while the purge is parked.
    read_task = asyncio.create_task(kvs.get_auto_saved_value('state', {'count': 0}))
    await asyncio.wait([read_task], timeout=0.1)
    assert not read_task.done()
    release_purge.set()
    await purge_task

    assert await read_task == {'count': 0}


@pytest.mark.parametrize('operation', [pytest.param('purge', id='purge'), pytest.param('drop', id='drop')])
async def test_failed_purge_or_drop_keeps_autosaved_values(
    kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """A purge or drop that the storage backend rejects leaves the autosaved values saved on `PERSIST_STATE`."""
    autosaved_value = await kvs.get_auto_saved_value('state', {'count': 0})
    purge_or_drop = getattr(kvs, operation)

    with monkeypatch.context() as failing_backend:
        failing_backend.setattr(kvs._backend, operation, AsyncMock(side_effect=NotImplementedError))
        with pytest.raises(NotImplementedError):
            await purge_or_drop()

    autosaved_value['count'] = 1
    await _emit_persist_state()

    assert await kvs.get_value('state') == {'count': 1}
    assert await kvs.get_auto_saved_value('state') is autosaved_value


async def test_failed_drop_keeps_store_cached(
    kvs: KeyValueStore, storage_backend: StorageBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A drop that the storage backend rejects leaves the store cached, so opening it again returns the same store."""
    with monkeypatch.context() as failing_backend:
        failing_backend.setattr(kvs._backend, 'drop', AsyncMock(side_effect=NotImplementedError))
        with pytest.raises(NotImplementedError):
            await kvs.drop()

    assert await KeyValueStore.open(storage_backend=storage_backend) is kvs


async def test_failed_purge_with_invalid_autosaved_record(kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed purge raises the storage backend's error when an autosaved value with an invalid record is cached."""
    await kvs.set_value('state', [1, 2, 3])
    with pytest.raises(ValidationError):
        await kvs.get_auto_saved_value('state')

    monkeypatch.setattr(kvs._backend, 'purge', AsyncMock(side_effect=NotImplementedError))
    with pytest.raises(NotImplementedError):
        await kvs.purge()


@pytest.mark.parametrize('operation', [pytest.param('purge', id='purge'), pytest.param('drop', id='drop')])
async def test_cancelled_purge_or_drop_keeps_autosaved_values(
    kvs: KeyValueStore, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """A purge or drop cancelled during the last save of an autosaved value leaves it saved on `PERSIST_STATE`."""
    autosaved_value = await kvs.get_auto_saved_value('state', {'count': 0})
    save_started = asyncio.Event()

    async def hanging_set_value(*_args: Any, **_kwargs: Any) -> None:
        save_started.set()
        await asyncio.sleep(3600)

    purge_or_drop = getattr(kvs, operation)

    with monkeypatch.context() as hanging_save:
        hanging_save.setattr(kvs, 'set_value', AsyncMock(side_effect=hanging_set_value))
        task = asyncio.create_task(purge_or_drop())
        await asyncio.wait_for(save_started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    autosaved_value['count'] = 1
    await _emit_persist_state()

    assert await kvs.get_value('state') == {'count': 1}
