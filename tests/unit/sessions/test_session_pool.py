from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

import pytest

from crawlee.events import EventManager
from crawlee.events.types import Event, EventPersistStateData
from crawlee.sessions import Session, SessionPool
from crawlee.sessions.models import SessionPoolModel
from crawlee.storages import KeyValueStore

MAX_POOL_SIZE = 3
KVS_NAME = 'test_session_pool'
PERSIST_STATE_KEY = 'crawlee_session_pool_state'


@pytest.fixture()
async def kvs() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open(name=KVS_NAME)
    yield kvs
    await kvs.drop()


@pytest.fixture()
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as em:
        yield em


@pytest.fixture()
async def session_pool() -> AsyncGenerator[SessionPool, None]:
    async with SessionPool(max_pool_size=MAX_POOL_SIZE, persistance_enabled=False) as sp:
        yield sp


async def test_session_pool_init(session_pool: SessionPool) -> None:
    assert session_pool.session_count == MAX_POOL_SIZE
    assert session_pool.usable_session_count == MAX_POOL_SIZE
    assert session_pool.retired_session_count == 0


async def test_add_session(session_pool: SessionPool) -> None:
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_02')
    session_pool.add_session(session=session_01)
    session_pool.add_session(session=session_02)
    assert session_pool.session_count == MAX_POOL_SIZE + 2
    assert session_pool.usable_session_count == MAX_POOL_SIZE + 2
    assert session_pool.retired_session_count == 0


async def test_add_session_duplicate(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_01')

    session_pool.add_session(session=session_01)

    with caplog.at_level(logging.WARNING):
        session_pool.add_session(session=session_02)


async def test_get_session(session_pool: SessionPool) -> None:
    session = await session_pool.get_session()
    assert session is not None
    assert session.expires_at >= datetime.now(timezone.utc)
    assert not session.is_blocked
    assert not session.is_expired
    assert not session.is_max_usage_count_reached
    assert session.is_usable


async def test_get_session_no_usable(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    session = await session_pool.get_session(session_id='non_existent')
    assert session is None

    session = Session(id='test_session_not_usable')
    session.retire()
    assert not session.is_usable
    session_pool.add_session(session=session)
    assert session_pool.session_count == MAX_POOL_SIZE + 1

    with caplog.at_level(logging.WARNING):
        session = await session_pool.get_session(session_id='test_session_not_usable')
        assert session is None


async def test_session_pool_persist(event_manager: EventManager, kvs: KeyValueStore) -> None:
    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        event_manager=event_manager,
        persistance_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        # Emit persist state event and wait for the persistence to complete
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        # Get the persisted state from the key-value store
        previous_state = await kvs.get_value(key=PERSIST_STATE_KEY)
        assert isinstance(previous_state, dict)
        sp_model = SessionPoolModel(**previous_state)

        # Check if the state is correctly persisted
        assert sp_model.session_count == sp.session_count
        assert sp_model.usable_session_count == sp.usable_session_count
        assert sp_model.retired_session_count == sp.retired_session_count

        # Check if all the sessions are correctly persisted
        for session_model in sp_model.sessions:
            kvs_session = Session.from_model(model=session_model)
            session = await sp.get_session(session_id=kvs_session.id)
            assert kvs_session == session


async def test_session_pool_persist_and_restore(event_manager: EventManager, kvs: KeyValueStore) -> None:
    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        event_manager=event_manager,
        persistance_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as _:
        # Emit persist state event and wait for the persistence to complete
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        event_manager=event_manager,
        persistance_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        # Not just reset the store and check it's empty
        await sp.reset_store()
        previous_state = await kvs.get_value(key=PERSIST_STATE_KEY)
        assert previous_state is None
