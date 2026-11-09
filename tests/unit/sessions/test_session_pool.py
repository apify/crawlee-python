from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest

from crawlee import service_locator
from crawlee.events import EventManager
from crawlee.events._types import Event, EventPersistStateData
from crawlee.sessions import Session, SessionPool
from crawlee.sessions._models import SessionPoolModel
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee import SessionReuseStrategy

MAX_POOL_SIZE = 3
KVS_NAME = 'test-session-pool'
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


@pytest.fixture(
    params=[
        pytest.param('random', id='random'),
        pytest.param('round-robin', id='round-robin'),
        pytest.param('use-until-failure', id='use-until-failure'),
    ]
)
def session_reuse_strategy(request: pytest.FixtureRequest) -> SessionReuseStrategy:
    """Provide each supported session reuse strategy."""
    return request.param


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


@pytest.mark.parametrize('use_default', [pytest.param(True, id='default'), pytest.param(False, id='explicit-random')])
async def test_random_session_reuse(monkeypatch: pytest.MonkeyPatch, *, use_default: bool) -> None:
    """Keep random selection and eager pool filling for the default and explicit random strategies."""
    choose = Mock(side_effect=lambda sessions: sessions[-1])
    monkeypatch.setattr('crawlee.sessions._session_pool.random.choice', choose)
    pool = (
        SessionPool(max_pool_size=MAX_POOL_SIZE)
        if use_default
        else SessionPool(max_pool_size=MAX_POOL_SIZE, session_reuse_strategy='random')
    )

    async with pool:
        session_ids = list(pool.get_state(as_dict=False).sessions)
        assert pool.session_count == MAX_POOL_SIZE
        assert (await pool.get_session()).id == session_ids[-1]
        assert (await pool.get_session()).id == session_ids[-1]
        assert choose.call_count == 2
        assert [session.id for session in choose.call_args.args[0]] == session_ids


def test_invalid_session_reuse_strategy() -> None:
    """Reject unsupported strategy names when constructing the pool."""
    with pytest.raises(ValueError, match='Invalid session reuse strategy'):
        SessionPool(session_reuse_strategy=cast('SessionReuseStrategy', 'unsupported'))


async def test_round_robin_session_reuse() -> None:
    """Cycle in insertion order and include externally added sessions without restarting the cycle."""
    async with SessionPool(max_pool_size=MAX_POOL_SIZE, session_reuse_strategy='round-robin') as pool:
        session_ids = list(pool.get_state(as_dict=False).sessions)
        assert [(await pool.get_session()).id for _ in range(2 * MAX_POOL_SIZE)] == session_ids * 2
        assert (await pool.get_session()).id == session_ids[0]

        extra_session = Session(id='extra-session')
        pool.add_session(extra_session)
        assert [(await pool.get_session()).id for _ in range(MAX_POOL_SIZE + 1)] == [
            *session_ids[1:],
            extra_session.id,
            session_ids[0],
        ]


async def test_use_until_failure_session_reuse() -> None:
    """Reuse the first usable session through successful uses and recoverable errors."""
    async with SessionPool(max_pool_size=MAX_POOL_SIZE, session_reuse_strategy='use-until-failure') as pool:
        session = await pool.get_session()
        assert session.id == next(iter(pool.get_state(as_dict=False).sessions))
        session.mark_good()
        session.mark_bad()
        assert session.is_usable
        assert await pool.get_session() is session
        assert await pool.get_session() is session


@pytest.mark.parametrize(
    'retired_index',
    [pytest.param(0, id='first'), pytest.param(1, id='middle'), pytest.param(2, id='last')],
)
async def test_round_robin_after_cleanup(retired_index: int) -> None:
    """Continue with the next surviving session after replacing any position in the cycle."""
    sessions = [Session() for _ in range(MAX_POOL_SIZE)]
    replacement = Session()
    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        session_reuse_strategy='round-robin',
        create_session_function=Mock(side_effect=[*sessions, replacement]),
    ) as pool:
        assert [await pool.get_session() for _ in range(retired_index)] == sessions[:retired_index]
        sessions[retired_index].retire()
        assert await pool.get_session() is replacement
        assert await pool.get_session() is sessions[(retired_index + 1) % MAX_POOL_SIZE]


@pytest.mark.parametrize(
    'failure',
    [
        pytest.param('retired', id='retired'),
        pytest.param('expired', id='expired'),
        pytest.param('max_usage', id='max-usage'),
        pytest.param('error_score', id='error-score'),
    ],
)
async def test_session_reuse_unusable_session(
    session_reuse_strategy: SessionReuseStrategy, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Respect each usability boundary and preserve selection order after removing invalid sessions."""
    first = Session(max_age=timedelta(minutes=1), max_usage_count=1, max_error_score=1)
    remaining = [Session(), Session()]
    replacement = Session()
    create_session = Mock(side_effect=[first, *remaining, replacement])
    monkeypatch.setattr('crawlee.sessions._session_pool.random.choice', Mock(side_effect=lambda sessions: sessions[0]))

    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        session_reuse_strategy=session_reuse_strategy,
        create_session_function=create_session,
    ) as pool:
        assert pool.session_count == MAX_POOL_SIZE
        assert await pool.get_session() is first
        if session_reuse_strategy == 'round-robin':
            assert [await pool.get_session() for _ in remaining] == remaining

        if failure == 'retired':
            first.retire()
        elif failure == 'expired':
            monkeypatch.setattr('crawlee.sessions._session.datetime', Mock(now=Mock(return_value=first.expires_at)))
        elif failure == 'max_usage':
            first.mark_good()
        else:
            first.mark_bad()

        assert not first.is_usable
        assert await pool.get_session_by_id(first.id) is None
        selected = await pool.get_session()
        assert selected.is_usable
        if session_reuse_strategy == 'use-until-failure':
            assert selected is remaining[0]
            assert create_session.call_count == MAX_POOL_SIZE
        else:
            assert selected is replacement
            assert first.id not in pool.get_state(as_dict=False).sessions
            assert create_session.call_count == MAX_POOL_SIZE + 1
            assert await pool.get_session() is remaining[0]


async def test_session_reuse_all_unusable(session_reuse_strategy: SessionReuseStrategy) -> None:
    """Replace a fully retired pool and refill it to capacity on the next retrieval."""
    async with SessionPool(max_pool_size=MAX_POOL_SIZE, session_reuse_strategy=session_reuse_strategy) as pool:
        session_ids = list(pool.get_state(as_dict=False).sessions)
        for session_id in session_ids:
            session = await pool.get_session_by_id(session_id)
            assert session is not None
            session.retire()

        replacement = await pool.get_session()
        assert replacement.is_usable
        assert replacement.id not in session_ids
        assert pool.session_count == 1
        assert (await pool.get_session()).is_usable
        assert pool.session_count == MAX_POOL_SIZE


async def test_session_reuse_reset_and_reopen(session_reuse_strategy: SessionReuseStrategy) -> None:
    """Restart selection from the beginning after resetting or reopening a pool."""
    pool = SessionPool(max_pool_size=MAX_POOL_SIZE, session_reuse_strategy=session_reuse_strategy)
    async with pool:
        await pool.get_session()
        await pool.reset_store()
        assert pool.session_count == 0
        selected = await pool.get_session()
        if session_reuse_strategy != 'random':
            assert selected.id == next(iter(pool.get_state(as_dict=False).sessions))

    async with pool:
        selected = await pool.get_session()
        if session_reuse_strategy != 'random':
            assert selected.id == next(iter(pool.get_state(as_dict=False).sessions))


async def test_session_reuse_persist_and_restore(
    event_manager: EventManager, kvs: KeyValueStore, session_reuse_strategy: SessionReuseStrategy
) -> None:
    """Restore session order and usability without adding strategy state to the persistence format."""
    service_locator.set_event_manager(event_manager)
    pool = SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        session_reuse_strategy=session_reuse_strategy,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    )
    async with pool:
        session_ids = list(pool.get_state(as_dict=False).sessions)
        session = await pool.get_session_by_id(session_ids[0])
        assert session is not None
        session.cookies.set('persisted-cookie', 'value', domain='example.com')
        await pool.get_session()
        session = await pool.get_session_by_id(session_ids[-1])
        assert session is not None
        session.retire()

    persisted = await kvs.get_value(PERSIST_STATE_KEY)
    assert isinstance(persisted, dict)
    assert set(persisted) == {'maxPoolSize', 'sessions', 'sessionCount', 'usableSessionCount', 'retiredSessionCount'}

    async with pool:
        assert list(pool.get_state(as_dict=False).sessions) == session_ids[:-1]
        selected = await pool.get_session()
        assert pool.session_count == MAX_POOL_SIZE
        if session_reuse_strategy != 'random':
            assert selected.id == session_ids[0]
            assert {cookie.name: cookie.value for cookie in selected.cookies.jar} == {'persisted-cookie': 'value'}


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
        for kvs_session in sp_model.sessions.values():
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

    with pytest.raises(RuntimeError, match=r'SessionPool is not active.'):
        session_pool.get_state(as_dict=True)

    with pytest.raises(RuntimeError, match=r'SessionPool is not active.'):
        session_pool.add_session(session)

    with pytest.raises(RuntimeError, match=r'SessionPool is not active.'):
        await session_pool.get_session()

    with pytest.raises(RuntimeError, match=r'SessionPool is not active.'):
        await session_pool.get_session_by_id(session.id)

    await session_pool.reset_store()

    with pytest.raises(RuntimeError, match=r'SessionPool is already active.'):
        async with session_pool, session_pool:
            pass

    async with session_pool:
        assert session_pool.active is True
