from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from itertools import chain, repeat
from typing import TYPE_CHECKING, cast
from unittest.mock import patch
from urllib.parse import urlparse

import pytest

from crawlee import service_locator
from crawlee.events import EventManager
from crawlee.storage_clients.models import StorageMetadata
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee._types import JsonSerializable


@pytest.fixture
async def mock_event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager(persist_state_interval=timedelta(milliseconds=50)) as event_manager:
        with patch('crawlee.service_locator.get_event_manager', return_value=event_manager):
            yield event_manager


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


async def test_open_save_storage_object() -> None:
    default_key_value_store = await KeyValueStore.open()

    assert default_key_value_store.storage_object is not None
    assert default_key_value_store.storage_object.id == default_key_value_store.id


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


async def test_get_public_url_raises_for_non_existing_key(key_value_store: KeyValueStore) -> None:
    with pytest.raises(ValueError, match='was not found'):
        await key_value_store.get_public_url('i-do-not-exist')


async def test_get_public_url(key_value_store: KeyValueStore) -> None:
    await key_value_store.set_value('test-static', 'static')
    public_url = await key_value_store.get_public_url('test-static')

    url = urlparse(public_url)
    path = url.netloc if url.netloc else url.path

    with open(path) as f:  # noqa: ASYNC230
        content = await asyncio.to_thread(f.read)
        assert content == 'static'


async def test_get_auto_saved_value_default_value(key_value_store: KeyValueStore) -> None:
    default_value: dict[str, JsonSerializable] = {'hello': 'world'}
    value = await key_value_store.get_auto_saved_value('state', default_value)
    assert value == default_value


async def test_get_auto_saved_value_cache_value(key_value_store: KeyValueStore) -> None:
    default_value: dict[str, JsonSerializable] = {'hello': 'world'}
    key_name = 'state'

    value = await key_value_store.get_auto_saved_value(key_name, default_value)
    value['hello'] = 'new_world'
    value_one = await key_value_store.get_auto_saved_value(key_name)
    assert value_one == {'hello': 'new_world'}

    value_one['hello'] = ['new_world']
    value_two = await key_value_store.get_auto_saved_value(key_name)
    assert value_two == {'hello': ['new_world']}


async def test_get_auto_saved_value_auto_save(key_value_store: KeyValueStore, mock_event_manager: EventManager) -> None:  # noqa: ARG001
    # This is not a realtime system and timing constrains can be hard to enforce.
    # For the test to avoid flakiness it needs some time tolerance.
    autosave_deadline_time = 1
    autosave_check_period = 0.01

    async def autosaved_within_deadline(key: str, expected_value: dict[str, str]) -> bool:
        """Check if the `key_value_store` of `key` has expected value within `autosave_deadline_time` seconds."""
        deadline = datetime.now(tz=timezone.utc) + timedelta(seconds=autosave_deadline_time)
        while datetime.now(tz=timezone.utc) < deadline:
            await asyncio.sleep(autosave_check_period)
            if await key_value_store.get_value(key) == expected_value:
                return True
        return False

    default_value: dict[str, JsonSerializable] = {'hello': 'world'}
    key_name = 'state'
    value = await key_value_store.get_auto_saved_value(key_name, default_value)
    assert await autosaved_within_deadline(key=key_name, expected_value={'hello': 'world'})

    value['hello'] = 'new_world'
    assert await autosaved_within_deadline(key=key_name, expected_value={'hello': 'new_world'})


async def test_get_auto_saved_value_auto_save_race_conditions(key_value_store: KeyValueStore) -> None:
    """Two parallel functions increment global variable obtained by `get_auto_saved_value`.

    Result should be incremented by 2.
    Method `get_auto_saved_value` must be implemented in a way that prevents race conditions in such scenario.
    Test creates situation where first `get_auto_saved_value` call to kvs gets delayed. Such situation can happen
    and unless handled, it can cause race condition in getting the state value."""
    await key_value_store.set_value('state', {'counter': 0})

    sleep_time_iterator = chain(iter([0.5]), repeat(0))

    async def delayed_get_value(key: str, default_value: None) -> None:
        await asyncio.sleep(next(sleep_time_iterator))
        return await KeyValueStore.get_value(key_value_store, key=key, default_value=default_value)

    async def increment_counter() -> None:
        state = cast('dict[str, int]', await key_value_store.get_auto_saved_value('state'))
        state['counter'] += 1

    with patch.object(key_value_store, 'get_value', delayed_get_value):
        tasks = [asyncio.create_task(increment_counter()), asyncio.create_task(increment_counter())]
        await asyncio.gather(*tasks)

    assert (await key_value_store.get_auto_saved_value('state'))['counter'] == 2


async def test_from_storage_object() -> None:
    storage_client = service_locator.get_storage_client()

    storage_object = StorageMetadata(
        id='dummy-id',
        name='dummy-name',
        accessed_at=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
        modified_at=datetime.now(timezone.utc),
        extra_attribute='extra',
    )

    key_value_store = KeyValueStore.from_storage_object(storage_client, storage_object)

    assert key_value_store.id == storage_object.id
    assert key_value_store.name == storage_object.name
    assert key_value_store.storage_object == storage_object
    assert storage_object.model_extra.get('extra_attribute') == 'extra'  # type: ignore[union-attr]
