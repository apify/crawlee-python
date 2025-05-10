from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storage_clients._memory import MemoryKeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def kvs_client() -> AsyncGenerator[MemoryKeyValueStoreClient, None]:
    """Fixture that provides a fresh memory key-value store client for each test."""
    client = await MemoryStorageClient().open_key_value_store_client(name='test_kvs')
    yield client
    await client.drop()


async def test_open_creates_new_kvs() -> None:
    """Test that open() creates a new key-value store with proper metadata and adds it to the cache."""
    client = await MemoryStorageClient().open_key_value_store_client(name='new_kvs')

    # Verify correct client type and properties
    assert isinstance(client, MemoryKeyValueStoreClient)
    assert client.metadata.id is not None
    assert client.metadata.name == 'new_kvs'
    assert isinstance(client.metadata.created_at, datetime)
    assert isinstance(client.metadata.accessed_at, datetime)
    assert isinstance(client.metadata.modified_at, datetime)


async def test_kvs_client_purge_on_start() -> None:
    """Test that purge_on_start=True clears existing data in the KVS."""
    configuration = Configuration(purge_on_start=True)

    # Create KVS and add data
    kvs_client1 = await MemoryStorageClient().open_key_value_store_client(
        name='test_purge_kvs',
        configuration=configuration,
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen
    kvs_client2 = await MemoryStorageClient().open_key_value_store_client(
        name='test_purge_kvs',
        configuration=configuration,
    )

    # Verify value was purged
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_open_with_id_and_name() -> None:
    """Test that open() can be used with both id and name parameters."""
    client = await MemoryStorageClient().open_key_value_store_client(
        id='some-id',
        name='some-name',
    )
    assert client.metadata.id == 'some-id'
    assert client.metadata.name == 'some-name'


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

    # Drop the store
    await kvs_client.drop()

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

    # Perform an operation that updates modified_at and accessed_at
    await kvs_client.set_value(key='new_key', value='new value')

    # Verify timestamps again
    assert kvs_client.metadata.created_at == initial_created
    assert kvs_client.metadata.modified_at > initial_modified
    assert kvs_client.metadata.accessed_at > accessed_after_get
