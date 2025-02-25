from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from crawlee import service_locator
from crawlee.events import EventManager
from crawlee.events._types import Event, EventPersistStateData
from crawlee.sessions import Session, SessionPool
from crawlee.sessions._models import SessionPoolModel
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

MAX_POOL_SIZE = 3
KVS_NAME = 'test_session_pool'
PERSIST_STATE_KEY = 'crawlee_session_pool_state'


@pytest.fixture
async def kvs() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open(name=KVS_NAME)
    yield kvs
    await kvs.drop()


@pytest.fixture
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as em:
        yield em


@pytest.fixture
async def session_pool() -> AsyncGenerator[SessionPool, None]:
    async with SessionPool(max_pool_size=MAX_POOL_SIZE, persistence_enabled=False) as sp:
        yield sp


async def test_session_pool_init(session_pool: SessionPool) -> None:
    """Ensure that the session pool initializes correctly with predefined parameters."""
    assert session_pool.session_count == MAX_POOL_SIZE
    assert session_pool.usable_session_count == MAX_POOL_SIZE
    assert session_pool.retired_session_count == 0


async def test_add_session(session_pool: SessionPool) -> None:
    """Test adding sessions to the session pool increases session counts appropriately."""
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_02')
    session_pool.add_session(session=session_01)
    session_pool.add_session(session=session_02)
    assert session_pool.session_count == MAX_POOL_SIZE + 2
    assert session_pool.usable_session_count == MAX_POOL_SIZE + 2
    assert session_pool.retired_session_count == 0


async def test_add_session_duplicate(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    """Verify that adding a duplicate session logs a warning and does not increase count."""
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_01')

    session_pool.add_session(session=session_01)
    assert session_pool.session_count == MAX_POOL_SIZE + 1

    with caplog.at_level(logging.WARNING):
        session_pool.add_session(session=session_02)

    assert session_pool.session_count == MAX_POOL_SIZE + 1


async def test_get_session(session_pool: SessionPool) -> None:
    """Check retrieval of a session from the pool and verify its properties."""
    session = await session_pool.get_session()
    assert session is not None
    assert session.expires_at >= datetime.now(timezone.utc)
    assert not session.is_blocked
    assert not session.is_expired
    assert not session.is_max_usage_count_reached
    assert session.is_usable


async def test_get_session_no_usable(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    """Ensure that retrieval of a non-existent or retired session returns None and logs warning."""
    session = await session_pool.get_session_by_id('non_existent')
    assert session is None

    session = Session(id='test_session_not_usable')
    session.retire()
    assert not session.is_usable
    session_pool.add_session(session=session)
    assert session_pool.session_count == MAX_POOL_SIZE + 1

    with caplog.at_level(logging.WARNING):
        session = await session_pool.get_session_by_id('test_session_not_usable')
        assert session is None


async def test_create_session_function() -> None:
    """Validate that a session created via a custom function works and has the expected fields set."""
    user_data = {'created_by': 'test_create_session_function'}
    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=False,
        create_session_function=lambda: Session(user_data=user_data),
    ) as sp:
        session = await sp.get_session()
        assert session is not None
        assert session.user_data == user_data


@pytest.mark.parametrize('kvs_name', [KVS_NAME, None])
async def test_session_pool_persist(event_manager: EventManager, kvs_name: str | None) -> None:
    """Test persistence of session pool state to KVS and validate stored data integrity."""
    service_locator.set_event_manager(event_manager)

    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=True,
        persist_state_kvs_name=kvs_name,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        # Emit persist state event and wait for the persistence to complete
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        # Get the persisted state from the key-value store
        kvs = await KeyValueStore.open(name=kvs_name)
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
            session = await sp.get_session_by_id(kvs_session.id)
            assert kvs_session == session


async def test_session_pool_persist_and_restore(event_manager: EventManager, kvs: KeyValueStore) -> None:
    """Check session pool's ability to persist its state and then restore it accurately after reset."""
    service_locator.set_event_manager(event_manager)

    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ):
        # Emit persist state event and wait for the persistence to complete
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        # Not just reset the store and check it's empty
        await sp.reset_store()
        previous_state = await kvs.get_value(key=PERSIST_STATE_KEY)
        assert previous_state is None


async def test_methods_raise_error_when_not_active() -> None:
    session = Session()
    session_pool = SessionPool()

    assert session_pool.active is False

    with pytest.raises(RuntimeError, match='SessionPool is not active.'):
        session_pool.get_state(as_dict=True)

    with pytest.raises(RuntimeError, match='SessionPool is not active.'):
        session_pool.add_session(session)

    with pytest.raises(RuntimeError, match='SessionPool is not active.'):
        await session_pool.get_session()

    with pytest.raises(RuntimeError, match='SessionPool is not active.'):
        await session_pool.get_session_by_id(session.id)

    await session_pool.reset_store()

    with pytest.raises(RuntimeError, match='SessionPool is already active.'):
        async with session_pool, session_pool:
            pass

    async with session_pool:
        assert session_pool.active is True
