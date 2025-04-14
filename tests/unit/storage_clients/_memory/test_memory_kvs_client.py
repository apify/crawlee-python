from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from crawlee.storage_clients._memory._key_value_store_client import MemoryKeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

pytestmark = pytest.mark.only


@pytest.fixture
async def kvs_client() -> AsyncGenerator[MemoryKeyValueStoreClient, None]:
    """Fixture that provides a fresh memory key-value store client for each test."""
    # Clear any existing key-value store clients in the cache
    MemoryKeyValueStoreClient._cache_by_name.clear()

    client = await MemoryKeyValueStoreClient.open(name='test_kvs')
    yield client
    await client.drop()

async def test_open_creates_new_store() -> None:
    """Test that open() creates a new key-value store with proper metadata and adds it to the cache."""
    client = await MemoryKeyValueStoreClient.open(name='new_kvs')

    # Verify client properties
    assert client.id is not None
    assert client.name == 'new_kvs'
    assert isinstance(client.created_at, datetime)
    assert isinstance(client.accessed_at, datetime)
    assert isinstance(client.modified_at, datetime)

    # Verify the client was cached
    assert 'new_kvs' in MemoryKeyValueStoreClient._cache_by_name


async def test_open_existing_store(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that open() loads an existing key-value store with matching properties."""
    # Open the same key-value store again
    reopened_client = await MemoryKeyValueStoreClient.open(name=kvs_client.name)

    # Verify client properties
    assert kvs_client.id == reopened_client.id
    assert kvs_client.name == reopened_client.name

    # Verify clients (python) ids
    assert id(kvs_client) == id(reopened_client)


async def test_open_with_id_and_name() -> None:
    """Test that open() can be used with both id and name parameters."""
    client = await MemoryKeyValueStoreClient.open(id='some-id', name='some-name')
    assert client.id == 'some-id'
    assert client.name == 'some-name'


@pytest.mark.parametrize(
    ('key', 'value', 'expected_content_type'),
    [
        pytest.param('string_key', 'string value', 'text/plain; charset=utf-8', id='string'),
        pytest.param('dict_key', {'name': 'test', 'value': 42}, 'application/json; charset=utf-8', id='dictionary'),
        pytest.param('list_key', [1, 2, 3], 'application/json; charset=utf-8', id='list'),
        pytest.param('bytes_key', b'binary data', 'application/octet-stream', id='bytes'),
    ],
)
async def test_set_get_value(
    kvs_client: MemoryKeyValueStoreClient,
    key: str,
    value: Any,
    expected_content_type: str,
) -> None:
    """Test storing and retrieving different types of values with correct content types."""
    # Set value
    await kvs_client.set_value(key=key, value=value)

    # Get and verify value
    record = await kvs_client.get_value(key=key)
    assert record is not None
    assert record.key == key
    assert record.value == value
    assert record.content_type == expected_content_type


async def test_get_nonexistent_value(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that attempting to get a non-existent key returns None."""
    record = await kvs_client.get_value(key='nonexistent')
    assert record is None


async def test_set_value_with_explicit_content_type(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that an explicitly provided content type overrides the automatically inferred one."""
    value = 'This could be XML'
    content_type = 'application/xml'

    await kvs_client.set_value(key='xml_key', value=value, content_type=content_type)

    record = await kvs_client.get_value(key='xml_key')
    assert record is not None
    assert record.value == value
    assert record.content_type == content_type


async def test_delete_value(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that a stored value can be deleted and is no longer retrievable after deletion."""
    # Set a value
    await kvs_client.set_value(key='delete_me', value='to be deleted')

    # Verify it exists
    record = await kvs_client.get_value(key='delete_me')
    assert record is not None

    # Delete it
    await kvs_client.delete_value(key='delete_me')

    # Verify it's gone
    record = await kvs_client.get_value(key='delete_me')
    assert record is None


async def test_delete_nonexistent_value(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that attempting to delete a non-existent key is a no-op and doesn't raise errors."""
    # Should not raise an error
    await kvs_client.delete_value(key='nonexistent')


async def test_iterate_keys(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that all keys can be iterated over and are returned in sorted order with correct metadata."""
    # Set some values
    items = {
        'a_key': 'value A',
        'b_key': 'value B',
        'c_key': 'value C',
        'd_key': 'value D',
    }

    for key, value in items.items():
        await kvs_client.set_value(key=key, value=value)

    # Get all keys
    metadata_list = [metadata async for metadata in kvs_client.iterate_keys()]

    # Verify keys are returned in sorted order
    assert len(metadata_list) == 4
    assert [m.key for m in metadata_list] == sorted(items.keys())
    assert all(isinstance(m, KeyValueStoreRecordMetadata) for m in metadata_list)


async def test_iterate_keys_with_exclusive_start_key(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that exclusive_start_key parameter returns only keys after it alphabetically."""
    # Set some values
    for key in ['a_key', 'b_key', 'c_key', 'd_key', 'e_key']:
        await kvs_client.set_value(key=key, value=f'value for {key}')

    # Get keys starting after 'b_key'
    metadata_list = [metadata async for metadata in kvs_client.iterate_keys(exclusive_start_key='b_key')]

    # Verify only keys after 'b_key' are returned
    assert len(metadata_list) == 3
    assert [m.key for m in metadata_list] == ['c_key', 'd_key', 'e_key']


async def test_iterate_keys_with_limit(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that the limit parameter returns only the specified number of keys."""
    # Set some values
    for key in ['a_key', 'b_key', 'c_key', 'd_key', 'e_key']:
        await kvs_client.set_value(key=key, value=f'value for {key}')

    # Get first 3 keys
    metadata_list = [metadata async for metadata in kvs_client.iterate_keys(limit=3)]

    # Verify only the first 3 keys are returned
    assert len(metadata_list) == 3
    assert [m.key for m in metadata_list] == ['a_key', 'b_key', 'c_key']


async def test_drop(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that drop removes the store from cache and clears all data."""
    # Add some values to the store
    await kvs_client.set_value(key='test', value='data')

    # Verify the store exists in the cache
    assert kvs_client.name in MemoryKeyValueStoreClient._cache_by_name

    # Drop the store
    await kvs_client.drop()

    # Verify the store was removed from the cache
    assert kvs_client.name not in MemoryKeyValueStoreClient._cache_by_name

    # Verify the store is empty
    record = await kvs_client.get_value(key='test')
    assert record is None


async def test_get_public_url(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that get_public_url raises NotImplementedError for the memory implementation."""
    with pytest.raises(NotImplementedError):
        await kvs_client.get_public_url(key='any-key')


async def test_metadata_updates(kvs_client: MemoryKeyValueStoreClient) -> None:
    """Test that read/write operations properly update accessed_at and modified_at timestamps."""
    # Record initial timestamps
    initial_created = kvs_client.created_at
    initial_accessed = kvs_client.accessed_at
    initial_modified = kvs_client.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates accessed_at
    await kvs_client.get_value(key='nonexistent')

    # Verify timestamps
    assert kvs_client.created_at == initial_created
    assert kvs_client.accessed_at > initial_accessed
    assert kvs_client.modified_at == initial_modified

    accessed_after_get = kvs_client.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform an operation that updates modified_at and accessed_at
    await kvs_client.set_value(key='new_key', value='new value')

    # Verify timestamps again
    assert kvs_client.created_at == initial_created
    assert kvs_client.modified_at > initial_modified
    assert kvs_client.accessed_at > accessed_after_get
