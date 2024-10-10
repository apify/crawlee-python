from __future__ import annotations

from pathlib import Path
from typing import AsyncGenerator
from urllib.parse import unquote, urlparse

import pytest

from crawlee.storages import KeyValueStore


@pytest.fixture
async def key_value_store() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open()
    yield kvs
    await kvs.drop()


async def test_open() -> None:
    default_key_value_store = await KeyValueStore.open()
    default_key_value_store_by_id = await KeyValueStore.open(id=default_key_value_store.id)

    assert default_key_value_store is default_key_value_store_by_id

    key_value_store_name = 'dummy-name'
    named_key_value_store = await KeyValueStore.open(name=key_value_store_name)
    assert default_key_value_store is not named_key_value_store

    with pytest.raises(RuntimeError, match='KeyValueStore with id "nonexistent-id" does not exist!'):
        await KeyValueStore.open(id='nonexistent-id')

    # Test that when you try to open a key-value store by ID and you use a name of an existing key-value store,
    # it doesn't work
    with pytest.raises(RuntimeError, match='KeyValueStore with id "dummy-name" does not exist!'):
        await KeyValueStore.open(id='dummy-name')


async def test_consistency_accross_two_clients() -> None:
    kvs = await KeyValueStore.open(name='my-kvs')
    await kvs.set_value('key', 'value')

    kvs_by_id = await KeyValueStore.open(id=kvs.id)
    await kvs_by_id.set_value('key2', 'value2')

    assert (await kvs.get_value('key')) == 'value'
    assert (await kvs.get_value('key2')) == 'value2'

    assert (await kvs_by_id.get_value('key')) == 'value'
    assert (await kvs_by_id.get_value('key2')) == 'value2'

    await kvs.drop()
    with pytest.raises(RuntimeError, match='Storage with provided ID was not found'):
        await kvs_by_id.drop()


async def test_same_references() -> None:
    kvs1 = await KeyValueStore.open()
    kvs2 = await KeyValueStore.open()
    assert kvs1 is kvs2

    kvs_name = 'non-default'
    kvs_named1 = await KeyValueStore.open(name=kvs_name)
    kvs_named2 = await KeyValueStore.open(name=kvs_name)
    assert kvs_named1 is kvs_named2


async def test_drop() -> None:
    kvs1 = await KeyValueStore.open()
    await kvs1.drop()
    kvs2 = await KeyValueStore.open()
    assert kvs1 is not kvs2


async def test_get_set_value(key_value_store: KeyValueStore) -> None:
    await key_value_store.set_value('test-str', 'string')
    await key_value_store.set_value('test-int', 123)
    await key_value_store.set_value('test-dict', {'abc': '123'})
    str_value = await key_value_store.get_value('test-str')
    int_value = await key_value_store.get_value('test-int')
    dict_value = await key_value_store.get_value('test-dict')
    non_existent_value = await key_value_store.get_value('test-non-existent')
    assert str_value == 'string'
    assert int_value == 123
    assert dict_value['abc'] == '123'
    assert non_existent_value is None


async def test_for_each_key(key_value_store: KeyValueStore) -> None:
    keys = [item.key async for item in key_value_store.iterate_keys()]
    assert len(keys) == 0

    for i in range(2001):
        await key_value_store.set_value(str(i).zfill(4), i)
    index = 0
    async for item in key_value_store.iterate_keys():
        assert item.key == str(index).zfill(4)
        index += 1
    assert index == 2001


async def test_static_get_set_value(key_value_store: KeyValueStore) -> None:
    await key_value_store.set_value('test-static', 'static')
    value = await key_value_store.get_value('test-static')
    assert value == 'static'


async def test_static_get_public_url(key_value_store: KeyValueStore) -> None:
    await key_value_store.set_value('test-static', 'static')
    public_url = key_value_store.get_public_url('test-static')

    path = unquote(urlparse(public_url).path)
    assert Path(path).read_text() == 'static'
