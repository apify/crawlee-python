from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storage_clients._memory import KeyValueStoreCollectionClient


@pytest.fixture
def key_value_stores_client(memory_storage_client: MemoryStorageClient) -> KeyValueStoreCollectionClient:
    return memory_storage_client.key_value_stores()


async def test_get_or_create(key_value_stores_client: KeyValueStoreCollectionClient) -> None:
    kvs_name = 'test'
    # A new kvs gets created
    kvs_info = await key_value_stores_client.get_or_create(name=kvs_name)
    assert kvs_info.name == kvs_name

    # Another get_or_create call returns the same kvs
    kvs_info_existing = await key_value_stores_client.get_or_create(name=kvs_name)
    assert kvs_info.id == kvs_info_existing.id
    assert kvs_info.name == kvs_info_existing.name
    assert kvs_info.created_at == kvs_info_existing.created_at


async def test_list(key_value_stores_client: KeyValueStoreCollectionClient) -> None:
    assert (await key_value_stores_client.list()).count == 0
    kvs_info = await key_value_stores_client.get_or_create(name='kvs')
    kvs_list = await key_value_stores_client.list()
    assert kvs_list.count == 1
    assert kvs_list.items[0].name == kvs_info.name

    # Test sorting behavior
    newer_kvs_info = await key_value_stores_client.get_or_create(name='newer-kvs')
    kvs_list_sorting = await key_value_stores_client.list()
    assert kvs_list_sorting.count == 2
    assert kvs_list_sorting.items[0].name == kvs_info.name
    assert kvs_list_sorting.items[1].name == newer_kvs_info.name
