from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee import service_locator
from crawlee.events import EventManager
from crawlee.events._types import Event, EventPersistStateData
from crawlee.proxy_configuration import ProxyConfiguration, ProxyInfo
from crawlee.sessions import Session, SessionOptions, SessionPool
from crawlee.sessions._models import SessionPoolModel
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

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


async def test_session_pool_init(session_pool: SessionPool) -> None:
    """Ensure that the session pool starts without sessions."""
    assert session_pool.session_count == 0
    assert session_pool.usable_session_count == 0
    assert session_pool.retired_session_count == 0


async def test_get_session_creates_sessions_up_to_max_pool_size(session_pool: SessionPool) -> None:
    """Test that `get_session` creates a new session until the pool is full and then reuses them."""
    new_sessions = list[Session]()
    for expected_count in range(1, MAX_POOL_SIZE + 1):
        new_sessions.append(await session_pool.get_session())
        assert session_pool.session_count == expected_count

    reused_session = await session_pool.get_session()

    assert len({session.id for session in new_sessions}) == MAX_POOL_SIZE
    assert reused_session in new_sessions
    assert session_pool.session_count == MAX_POOL_SIZE


async def test_add_session(session_pool: SessionPool) -> None:
    """Test adding sessions to the session pool increases session counts appropriately."""
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_02')
    session_pool.add_session(session=session_01)
    session_pool.add_session(session=session_02)
    assert session_pool.session_count == 2
    assert session_pool.usable_session_count == 2
    assert session_pool.retired_session_count == 0


async def test_add_session_duplicate(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    """Verify that adding a duplicate session logs a warning and does not increase count."""
    session_01 = Session(id='test_session_01')
    session_02 = Session(id='test_session_01')

    session_pool.add_session(session=session_01)
    assert session_pool.session_count == 1

    with caplog.at_level(logging.WARNING):
        session_pool.add_session(session=session_02)

    assert session_pool.session_count == 1


async def test_get_session(session_pool: SessionPool) -> None:
    """Check retrieval of a session from the pool and verify its properties."""
    session = await session_pool.get_session()
    assert session is not None
    assert session.expires_at >= datetime.now(UTC)
    assert not session.is_blocked
    assert not session.is_expired
    assert not session.is_max_usage_count_reached
    assert session.is_usable


async def test_pool_does_not_reuse_retired_session_after_success() -> None:
    """Neither pool lookup returns a retired session after a successful request."""
    async with SessionPool(max_pool_size=1, persistence_enabled=False) as pool:
        session = await pool.get_session()
        session.retire()
        session.mark_good()

        assert await pool.get_session_by_id(session.id) is None
        replacement = await pool.get_session()
        assert replacement.id != session.id
        assert replacement.is_usable


async def test_get_session_no_usable(caplog: pytest.LogCaptureFixture, session_pool: SessionPool) -> None:
    """Ensure that retrieval of a non-existent or retired session returns None and logs warning."""
    session = await session_pool.get_session_by_id('non_existent')
    assert session is None

    session = Session(id='test_session_not_usable')
    session.retire()
    assert not session.is_usable
    session_pool.add_session(session=session)
    assert session_pool.session_count == 1

    with caplog.at_level(logging.WARNING):
        session = await session_pool.get_session_by_id('test_session_not_usable')
        assert session is None


async def test_create_session_function() -> None:
    """Validate that a session created via a custom function works and has the expected fields set."""
    user_data = {'created_by': 'test_create_session_function'}
    async with SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=False,
        create_session_function=lambda _: Session(user_data=user_data),
    ) as sp:
        session = await sp.get_session()
        assert session is not None
        assert session.user_data == user_data


@pytest.mark.parametrize(
    'mock_type',
    [pytest.param(Mock, id='sync-function'), pytest.param(AsyncMock, id='async-function')],
)
async def test_create_session_function_gets_settings(mock_type: type[Mock]) -> None:
    """Test that a sync or async session factory gets `create_session_settings` as its options."""
    create_session_function = mock_type(side_effect=lambda options: Session(**options))

    async with SessionPool(
        max_pool_size=1,
        create_session_settings={'max_usage_count': 5},
        create_session_function=create_session_function,
    ) as sp:
        await sp.get_session()

    create_session_function.assert_called_once_with({'max_usage_count': 5})


async def test_proxy_configuration_sets_proxy_info_of_new_sessions() -> None:
    """Test that the pool passes a proxy from its `proxy_configuration` to the factory of every new session."""
    create_session_function = Mock(side_effect=lambda options: Session(**options))
    proxy_urls: list[str | None] = ['http://proxy-1.com:8000', 'http://proxy-2.com:8000']

    async with SessionPool(
        max_pool_size=2,
        create_session_function=create_session_function,
        proxy_configuration=ProxyConfiguration(proxy_urls=proxy_urls),
    ) as sp:
        await sp.get_session()
        await sp.get_session()

    factory_proxy_infos = [call.args[0]['proxy_info'] for call in create_session_function.call_args_list]
    assert [proxy_info.url for proxy_info in factory_proxy_infos] == proxy_urls


async def test_restored_session_is_recreated_by_factory() -> None:
    """Test that a restored session goes through the factory with its state and the refreshed persisted proxy."""
    persisted_proxy_info = ProxyInfo(
        url='http://old-proxy.com:8000', scheme='http', hostname='old-proxy.com', port=8000
    )
    refreshed_proxy_info = ProxyInfo(
        url='http://new-proxy.com:8000', scheme='http', hostname='new-proxy.com', port=8000
    )

    async with SessionPool(
        max_pool_size=1,
        create_session_function=lambda _: Session(
            id='restored', cookies={'token': 'abc'}, proxy_info=persisted_proxy_info
        ),
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        await sp.get_session()

    proxy_configuration = Mock(spec=ProxyConfiguration)
    proxy_configuration.new_proxy_info = AsyncMock(return_value=refreshed_proxy_info)
    create_session_function = Mock(side_effect=lambda options: Session(**options))

    async with SessionPool(
        max_pool_size=1,
        create_session_function=create_session_function,
        proxy_configuration=proxy_configuration,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        session = await sp.get_session_by_id('restored')

    proxy_configuration.new_proxy_info.assert_awaited_once_with(persisted_proxy_info)
    create_session_function.assert_called_once()
    assert session is not None
    assert session.proxy_info == refreshed_proxy_info
    assert session.cookies.get_cookies_as_dicts()[0]['value'] == 'abc'


async def test_restored_state_overrides_create_session_settings() -> None:
    """Test that the state of a restored session takes precedence over `create_session_settings`."""
    async with SessionPool(
        max_pool_size=1,
        create_session_settings={'cookies': {'token': 'restored'}},
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        session = await sp.get_session()

    async with SessionPool(
        max_pool_size=1,
        create_session_settings={'cookies': {'token': 'default'}},
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        restored_session = await sp.get_session_by_id(session.id)

    assert restored_session is not None
    assert restored_session.cookies.get_cookies_as_dicts()[0]['value'] == 'restored'


async def test_restored_session_keeps_proxy_without_proxy_configuration() -> None:
    """Test that a restored session keeps its persisted proxy in a pool without `proxy_configuration`."""
    proxy_info = ProxyInfo(url='http://proxy.com:8000', scheme='http', hostname='proxy.com', port=8000)

    async with SessionPool(
        max_pool_size=1,
        create_session_settings={'proxy_info': proxy_info},
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        session = await sp.get_session()

    async with SessionPool(
        max_pool_size=1,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        restored_session = await sp.get_session_by_id(session.id)

    assert restored_session is not None
    assert restored_session.proxy_info == proxy_info


async def test_restore_is_retried_after_failed_entry() -> None:
    """Test that the restored sessions go through the factory again when the factory failed on the previous entry."""
    async with SessionPool(
        max_pool_size=1,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as sp:
        await sp.get_session()

    recreated_session = Session()
    session_pool = SessionPool(
        max_pool_size=1,
        create_session_function=Mock(side_effect=[RuntimeError('factory failed'), recreated_session]),
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    )

    with pytest.raises(RuntimeError, match='factory failed'):
        async with session_pool:
            pass

    async with session_pool:
        assert await session_pool.get_session() is recreated_session


async def test_reentering_does_not_recreate_sessions() -> None:
    """Test that entering the pool again doesn't pass its sessions through the factory again."""
    create_session_function = Mock(side_effect=lambda options: Session(**options))
    session_pool = SessionPool(max_pool_size=MAX_POOL_SIZE, create_session_function=create_session_function)

    async with session_pool:
        await session_pool.get_session()

    async with session_pool:
        pass

    assert create_session_function.call_count == 1


async def test_concurrent_get_session_does_not_overfill_pool() -> None:
    """Test that concurrent `get_session` calls with an async factory keep the pool within its size."""

    async def create_session(options: SessionOptions) -> Session:
        await asyncio.sleep(0)
        return Session(**options)

    async with SessionPool(max_pool_size=1, create_session_function=create_session) as sp:
        session = await sp.get_session()
        session.retire()

        sessions = await asyncio.gather(sp.get_session(), sp.get_session())

        assert sp.session_count == 1
        assert sessions[0] is sessions[1]


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
        for _ in range(MAX_POOL_SIZE):
            await sp.get_session()

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
        with pytest.raises(RuntimeError, match=r'Use reset\(\) to reset the state itself'):
            await sp.reset_store()

    # The record can be cleared only once the pool is exited, since its final persist would write it back.
    await sp.reset_store()
    previous_state = await kvs.get_value(key=PERSIST_STATE_KEY)
    assert previous_state is None


async def test_reset_discards_sessions_and_keeps_record(kvs: KeyValueStore) -> None:
    """`reset` discards the sessions in memory and leaves the persisted record alone."""
    session_pool = SessionPool(
        max_pool_size=MAX_POOL_SIZE,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    )
    async with session_pool:
        for _ in range(MAX_POOL_SIZE):
            await session_pool.get_session()

    session_pool.reset()

    assert session_pool.session_count == 0
    saved_pool = SessionPoolModel.model_validate(await kvs.get_value(PERSIST_STATE_KEY))
    assert len(saved_pool.sessions) == MAX_POOL_SIZE


async def test_reentering_keeps_sessions() -> None:
    """Entering the pool again keeps the sessions of the previous entry."""
    session_pool = SessionPool(max_pool_size=MAX_POOL_SIZE)

    async with session_pool:
        session = await session_pool.get_session()

    async with session_pool:
        assert set(session_pool.get_state(as_dict=False).sessions) == {session.id}


async def test_retired_session_is_not_restored(event_manager: EventManager, kvs: KeyValueStore) -> None:
    """A saved retired session is excluded when the pool is reopened."""
    service_locator.set_event_manager(event_manager)

    async with SessionPool(
        max_pool_size=1,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as pool:
        session = await pool.get_session()
        session.retire()
        session.mark_good()
        event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
        await event_manager.wait_for_all_listeners_to_complete()

        saved_state = await kvs.get_value(PERSIST_STATE_KEY)
        assert isinstance(saved_state, dict)
        saved_pool = SessionPoolModel.model_validate(saved_state)
        assert saved_pool.sessions[session.id].retired
        assert not saved_pool.sessions[session.id].is_usable

    async with SessionPool(
        max_pool_size=1,
        persistence_enabled=True,
        persist_state_kvs_name=KVS_NAME,
        persist_state_key=PERSIST_STATE_KEY,
    ) as restored_pool:
        assert await restored_pool.get_session_by_id(session.id) is None
        replacement = await restored_pool.get_session()
        assert replacement.id != session.id
        assert replacement.is_usable


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


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('enter failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_enter_frees_pool(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """A pool whose entry fails or is cancelled tears its state down, stays inactive and can be entered again."""
    session_pool = SessionPool(max_pool_size=MAX_POOL_SIZE, persistence_enabled=False)
    teardown = AsyncMock(wraps=session_pool._state.teardown)
    monkeypatch.setattr(session_pool._state, 'teardown', teardown)
    monkeypatch.setattr(session_pool, '_remove_retired_sessions', Mock(side_effect=error))

    with pytest.raises(type(error)):
        async with session_pool:
            pass

    assert not session_pool.active
    teardown.assert_awaited_once()
    monkeypatch.undo()

    async with session_pool:
        assert session_pool.active


async def test_failed_exit_frees_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pool whose exit fails is no longer active, so it can be entered again."""
    session_pool = SessionPool(max_pool_size=MAX_POOL_SIZE, persistence_enabled=False)

    with pytest.raises(RuntimeError, match='teardown failed'):
        async with session_pool:
            monkeypatch.setattr(session_pool._state, 'teardown', AsyncMock(side_effect=RuntimeError('teardown failed')))

    assert not session_pool.active
    monkeypatch.undo()

    async with session_pool:
        assert session_pool.active


async def test_enter_during_exit_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pool can't be entered again until its exit has torn the state down."""
    session_pool = SessionPool(max_pool_size=MAX_POOL_SIZE, persistence_enabled=False)
    teardown_started = asyncio.Event()
    release_teardown = asyncio.Event()

    async def wait_for_release() -> None:
        teardown_started.set()
        await release_teardown.wait()

    await session_pool.__aenter__()
    monkeypatch.setattr(session_pool._state, 'teardown', AsyncMock(side_effect=wait_for_release))
    exit_task = asyncio.create_task(session_pool.__aexit__(None, None, None))
    await teardown_started.wait()

    with pytest.raises(RuntimeError, match='already active'):
        await session_pool.__aenter__()

    release_teardown.set()
    await exit_task
