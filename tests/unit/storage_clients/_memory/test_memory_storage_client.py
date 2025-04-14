from __future__ import annotations

import pytest

from crawlee.storage_clients._memory._dataset_client import MemoryDatasetClient
from crawlee.storage_clients._memory._key_value_store_client import MemoryKeyValueStoreClient
from crawlee.storage_clients._memory._storage_client import MemoryStorageClient

pytestmark = pytest.mark.only


@pytest.fixture
async def client() -> MemoryStorageClient:
    return MemoryStorageClient()


async def test_open_dataset_client(client: MemoryStorageClient) -> None:
    """Test that open_dataset_client creates a dataset client with correct type and properties."""
    dataset_client = await client.open_dataset_client(name='test-dataset')

    # Verify correct client type and properties
    assert isinstance(dataset_client, MemoryDatasetClient)
    assert dataset_client.name == 'test-dataset'
    assert dataset_client.item_count == 0


async def test_dataset_client_purge_on_start(client: MemoryStorageClient) -> None:
    """Test that purge_on_start=True clears existing data in the dataset."""
    # Create dataset and add data
    dataset_client1 = await client.open_dataset_client(
        name='test-purge-dataset',
        purge_on_start=True,
    )
    await dataset_client1.push_data({'item': 'initial data'})

    # Verify data was added
    items = await dataset_client1.get_data()
    assert len(items.items) == 1

    # Reopen
    dataset_client2 = await client.open_dataset_client(
        name='test-purge-dataset',
        purge_on_start=True,
    )

    # Verify data was purged
    items = await dataset_client2.get_data()
    assert len(items.items) == 0


async def test_dataset_client_no_purge_on_start(client: MemoryStorageClient) -> None:
    """Test that purge_on_start=False keeps existing data in the dataset."""
    # Create dataset and add data
    dataset_client1 = await client.open_dataset_client(
        name='test-no-purge-dataset',
        purge_on_start=False,
    )
    await dataset_client1.push_data({'item': 'preserved data'})

    # Reopen
    dataset_client2 = await client.open_dataset_client(
        name='test-no-purge-dataset',
        purge_on_start=False,
    )

    # Verify data was preserved
    items = await dataset_client2.get_data()
    assert len(items.items) == 1
    assert items.items[0]['item'] == 'preserved data'


async def test_open_kvs_client(client: MemoryStorageClient) -> None:
    """Test that open_key_value_store_client creates a KVS client with correct type and properties."""
    kvs_client = await client.open_key_value_store_client(name='test-kvs')

    # Verify correct client type and properties
    assert isinstance(kvs_client, MemoryKeyValueStoreClient)
    assert kvs_client.name == 'test-kvs'


async def test_kvs_client_purge_on_start(client: MemoryStorageClient) -> None:
    """Test that purge_on_start=True clears existing data in the KVS."""
    # Create KVS and add data
    kvs_client1 = await client.open_key_value_store_client(
        name='test-purge-kvs',
        purge_on_start=True,
    )
    await kvs_client1.set_value(key='test-key', value='initial value')

    # Verify value was set
    record = await kvs_client1.get_value(key='test-key')
    assert record is not None
    assert record.value == 'initial value'

    # Reopen
    kvs_client2 = await client.open_key_value_store_client(
        name='test-purge-kvs',
        purge_on_start=True,
    )

    # Verify value was purged
    record = await kvs_client2.get_value(key='test-key')
    assert record is None


async def test_kvs_client_no_purge_on_start(client: MemoryStorageClient) -> None:
    """Test that purge_on_start=False keeps existing data in the KVS."""
    # Create KVS and add data
    kvs_client1 = await client.open_key_value_store_client(
        name='test-no-purge-kvs',
        purge_on_start=False,
    )
    await kvs_client1.set_value(key='test-key', value='preserved value')

    # Reopen
    kvs_client2 = await client.open_key_value_store_client(
        name='test-no-purge-kvs',
        purge_on_start=False,
    )

    # Verify value was preserved
    record = await kvs_client2.get_value(key='test-key')
    assert record is not None
    assert record.value == 'preserved value'
