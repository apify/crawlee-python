from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, ValidationError

from crawlee import service_locator
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.events import EventManager
from crawlee.events._types import Event, EventPersistStateData
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

KEY = 'test-key'

logger = logging.getLogger(__name__)


class _State(BaseModel):
    counter: int = 0
    message: str = 'hello'


@pytest.fixture
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as event_manager:
        service_locator.set_event_manager(event_manager)
        yield event_manager


def _build_state(
    *, persistence_enabled: bool = True, persistence_timeout: timedelta = timedelta(seconds=60)
) -> RecoverableState[_State]:
    return RecoverableState(
        default_state=_State(),
        persist_state_key=KEY,
        persistence_enabled=persistence_enabled,
        persistence_timeout=persistence_timeout,
        logger=logger,
    )


async def _emit_persist_state(event_manager: EventManager) -> None:
    event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
    await event_manager.wait_for_all_listeners_to_complete()


async def test_initialize_without_persistence_uses_copy_of_default_state() -> None:
    """Without persistence, `initialize` establishes a copy of the default state."""
    default_state = _State()
    recoverable_state = RecoverableState(
        default_state=default_state, persist_state_key=KEY, persistence_enabled=False, logger=logger
    )

    state = await recoverable_state.initialize()

    assert state == default_state
    assert state is not default_state


def test_current_value_raises_before_state_is_established() -> None:
    """Reading the state before `initialize` or `reset` raises."""
    recoverable_state = _build_state()

    with pytest.raises(RuntimeError, match=r'Recoverable state has not yet been loaded'):
        _ = recoverable_state.current_value


def test_reset_establishes_default_state() -> None:
    """`reset` establishes a copy of the default state without `initialize`."""
    recoverable_state = _build_state()

    recoverable_state.reset()

    assert recoverable_state.current_value == _State()
    assert recoverable_state.is_initialized is False


async def test_initialize_keeps_earlier_state_without_record() -> None:
    """A state established before `initialize` survives it when there is no record to restore."""
    recoverable_state = _build_state()
    recoverable_state.reset()
    recoverable_state.current_value.counter = 42

    state = await recoverable_state.initialize()

    assert state.counter == 42
    await recoverable_state.teardown()


async def test_initialize_restores_record_over_earlier_state() -> None:
    """A restored record replaces a state established before `initialize`."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 7, 'message': 'hello'})

    recoverable_state = _build_state()
    recoverable_state.reset()
    recoverable_state.current_value.counter = 42

    state = await recoverable_state.initialize()

    assert state.counter == 7
    await recoverable_state.teardown()


async def test_reset_restores_default_state() -> None:
    """`reset` replaces a modified state with a fresh copy of the default state."""
    recoverable_state = _build_state(persistence_enabled=False)
    state = await recoverable_state.initialize()
    state.counter = 42

    recoverable_state.reset()

    assert recoverable_state.current_value == _State()
    assert recoverable_state.current_value is not state


async def test_teardown_without_persistence() -> None:
    """`teardown` without persistence keeps the state and marks the object as not initialized."""
    recoverable_state = _build_state(persistence_enabled=False)
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42

    await recoverable_state.teardown()

    assert recoverable_state.is_initialized is False
    assert recoverable_state.current_value.counter == 42


async def test_initialize_is_idempotent() -> None:
    """A repeated `initialize` keeps the in-memory state."""
    recoverable_state = _build_state(persistence_enabled=False)
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42

    state = await recoverable_state.initialize()

    assert state.counter == 42


async def test_initialize_after_teardown_keeps_in_memory_state() -> None:
    """`initialize` after `teardown` keeps the in-memory state instead of reloading the record."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42
    await recoverable_state.teardown()

    await kvs.set_value(KEY, {'counter': 7, 'message': 'hello'})
    state = await recoverable_state.initialize()

    assert state.counter == 42
    await recoverable_state.teardown()


async def test_initialize_after_teardown_listens_again(event_manager: EventManager) -> None:
    """`initialize` after `teardown` persists on PERSIST_STATE events again."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await recoverable_state.teardown()

    recoverable_state.current_value.counter = 1
    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 0, 'message': 'hello'}

    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 1, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_persist_state_event_during_load_keeps_record(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PERSIST_STATE event while the record is loading doesn't overwrite it with the default state."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = kvs.get_value

    async def get_value_after_event(key: str) -> object:
        await _emit_persist_state(event_manager)
        return await get_value(key)

    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=get_value_after_event))
    recoverable_state = _build_state()

    state = await recoverable_state.initialize()

    assert state.counter == 42
    await recoverable_state.teardown()


async def test_cancelled_load_keeps_record_and_is_retried(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A load cancelled in `initialize` keeps the record, even through teardown, and the next `initialize` loads it."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = kvs.get_value
    load_started = asyncio.Event()

    async def hanging_get_value(_key: str) -> None:
        load_started.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=hanging_get_value))
    recoverable_state = _build_state()
    initialize_task = asyncio.create_task(recoverable_state.initialize())
    await load_started.wait()
    initialize_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await initialize_task

    await _emit_persist_state(event_manager)
    await recoverable_state.teardown()
    assert await get_value(KEY) == {'counter': 42, 'message': 'hello'}

    monkeypatch.setattr(kvs, 'get_value', get_value)
    state = await recoverable_state.initialize()

    assert state.counter == 42
    await recoverable_state.teardown()


async def test_failed_load_keeps_record_and_is_retried(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A load failing in `initialize` leaves the object unwired and the record intact, and the next call loads it."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = kvs.get_value
    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=OSError('store is gone')))
    recoverable_state = _build_state()

    with pytest.raises(OSError, match=r'store is gone'):
        await recoverable_state.initialize()

    assert recoverable_state.is_initialized is False
    await _emit_persist_state(event_manager)
    await recoverable_state.teardown()
    assert await get_value(KEY) == {'counter': 42, 'message': 'hello'}

    monkeypatch.setattr(kvs, 'get_value', get_value)
    state = await recoverable_state.initialize()

    assert state.counter == 42
    await recoverable_state.teardown()


async def test_failed_persist_on_event_logs_warning(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed persist on a PERSIST_STATE event logs a warning, while a direct `persist_state` call raises."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=RuntimeError('store is on fire')))

    with caplog.at_level(logging.WARNING):
        await _emit_persist_state(event_manager)

    assert any(record.message == f"Failed to persist the state under key '{KEY}'." for record in caplog.records)
    with pytest.raises(RuntimeError, match=r'store is on fire'):
        await recoverable_state.persist_state()


async def test_failed_persist_in_teardown_logs_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed persist in `teardown` logs a warning instead of raising."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=RuntimeError('store is on fire')))

    with caplog.at_level(logging.WARNING):
        await recoverable_state.teardown()

    assert any(record.message == f"Failed to persist the state under key '{KEY}'." for record in caplog.records)


async def test_persisted_state_is_restored() -> None:
    """A persisted state is restored by a new instance."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42

    await recoverable_state.persist_state()

    assert await kvs.get_value(KEY) == {'counter': 42, 'message': 'hello'}
    assert (await _build_state().initialize()).counter == 42


async def test_invalid_record_raises_and_keeps_record(event_manager: EventManager) -> None:
    """An invalid record makes every `initialize` raise and stays intact until `reset_store` clears it."""
    kvs = await KeyValueStore.open()
    invalid_record = {'counter': 'not a number', 'message': 'hello'}
    await kvs.set_value(KEY, invalid_record)
    recoverable_state = _build_state()

    with pytest.raises(ValidationError) as exc_info:
        await recoverable_state.initialize()

    assert any(KEY in note for note in exc_info.value.__notes__)
    assert recoverable_state.is_initialized is False
    await _emit_persist_state(event_manager)
    await recoverable_state.teardown()
    assert await kvs.get_value(KEY) == invalid_record
    with pytest.raises(ValidationError):
        await recoverable_state.initialize()

    await recoverable_state.reset_store()
    state = await recoverable_state.initialize()

    assert state == _State()
    await recoverable_state.teardown()


async def test_reset_leaves_record_alone() -> None:
    """`reset` resets the in-memory state only."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42
    await recoverable_state.persist_state()

    recoverable_state.reset()

    assert recoverable_state.current_value.counter == 0
    assert await kvs.get_value(KEY) == {'counter': 42, 'message': 'hello'}
    await recoverable_state.teardown()


async def test_reset_is_undone_by_next_load_unless_record_is_cleared() -> None:
    """After `reset`, a new instance still restores the record, and after `reset_store` it doesn't."""
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42
    await recoverable_state.teardown()

    recoverable_state.reset()
    assert (await _build_state().initialize()).counter == 42

    await recoverable_state.reset_store()
    assert (await _build_state().initialize()).counter == 0


async def test_reset_store_clears_record_and_keeps_in_memory_state() -> None:
    """`reset_store` clears the record but leaves the in-memory state alone."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    recoverable_state.current_value.counter = 42
    await recoverable_state.teardown()

    await recoverable_state.reset_store()

    assert await kvs.get_value(KEY) is None
    assert recoverable_state.current_value.counter == 42


async def test_reset_store_raises_while_listening() -> None:
    """`reset_store` raises between `initialize` and `teardown`, since the next persist would restore the record."""
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    with pytest.raises(RuntimeError, match=r'Use reset\(\) to reset the state itself'):
        await recoverable_state.reset_store()

    await recoverable_state.teardown()
    await recoverable_state.reset_store()


async def test_reset_store_during_load_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """`reset_store` called while `initialize` is loading the record waits for the load and raises."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = kvs.get_value
    load_started = asyncio.Event()
    finish_load = asyncio.Event()

    async def slow_get_value(key: str) -> object:
        load_started.set()
        await finish_load.wait()
        return await get_value(key)

    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=slow_get_value))
    recoverable_state = _build_state()
    initialize_task = asyncio.create_task(recoverable_state.initialize())
    await load_started.wait()
    reset_store_task = asyncio.create_task(recoverable_state.reset_store())
    await asyncio.sleep(0)
    finish_load.set()
    await initialize_task

    with pytest.raises(RuntimeError, match=r'Use reset\(\) to reset the state itself'):
        await reset_store_task

    assert await get_value(KEY) == {'counter': 42, 'message': 'hello'}
    await recoverable_state.teardown()


async def test_reset_store_before_initialize_clears_record() -> None:
    """`reset_store` before the first `initialize` clears the record in the store."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    recoverable_state = _build_state()

    await recoverable_state.reset_store()

    assert await kvs.get_value(KEY) is None
    assert (await recoverable_state.initialize()).counter == 0
    await recoverable_state.teardown()


async def test_persist_state_before_initialize_does_nothing() -> None:
    """`persist_state` before `initialize` writes nothing."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    recoverable_state.reset()

    await recoverable_state.persist_state()

    assert await kvs.get_value(KEY) is None


async def test_persist_state_without_persistence_does_nothing() -> None:
    """`persist_state` without persistence writes nothing."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state(persistence_enabled=False)
    await recoverable_state.initialize()

    await recoverable_state.persist_state()

    assert await kvs.get_value(KEY) is None


async def test_persist_state_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persist that takes longer than `persistence_timeout` raises `TimeoutError`."""
    kvs = await KeyValueStore.open()
    # The short timeout covers the load in `initialize` too, so the real read must not run under it.
    monkeypatch.setattr(kvs, 'get_value', AsyncMock(return_value=None))
    recoverable_state = _build_state(persistence_timeout=timedelta(milliseconds=50))
    await recoverable_state.initialize()

    async def slow_set_value(*_args: object, **_kwargs: object) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=slow_set_value))

    with pytest.raises(TimeoutError, match=r"Persisting the state under key 'test-key' timed out after 0.05 seconds."):
        await recoverable_state.persist_state()


async def test_concurrent_initialize_loads_record_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Concurrent `initialize` calls load the record once and return the same state."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = AsyncMock(side_effect=kvs.get_value)
    monkeypatch.setattr(kvs, 'get_value', get_value)
    recoverable_state = _build_state()

    first, second = await asyncio.gather(recoverable_state.initialize(), recoverable_state.initialize())

    assert first is second
    assert first.counter == 42
    get_value.assert_awaited_once()
    await recoverable_state.teardown()


async def test_teardown_during_load_waits_for_it(event_manager: EventManager, monkeypatch: pytest.MonkeyPatch) -> None:
    """`teardown` called while `initialize` is loading the record waits for it and leaves the object torn down."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    get_value = kvs.get_value
    load_started = asyncio.Event()
    finish_load = asyncio.Event()

    async def slow_get_value(key: str) -> object:
        load_started.set()
        await finish_load.wait()
        return await get_value(key)

    monkeypatch.setattr(kvs, 'get_value', AsyncMock(side_effect=slow_get_value))
    recoverable_state = _build_state()
    initialize_task = asyncio.create_task(recoverable_state.initialize())
    await load_started.wait()
    teardown_task = asyncio.create_task(recoverable_state.teardown())
    await asyncio.sleep(0)
    finish_load.set()
    await asyncio.gather(initialize_task, teardown_task)

    assert recoverable_state.is_initialized is False
    recoverable_state.current_value.counter = 1
    await _emit_persist_state(event_manager)
    assert await get_value(KEY) == {'counter': 42, 'message': 'hello'}
