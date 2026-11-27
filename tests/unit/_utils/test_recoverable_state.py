from __future__ import annotations

import asyncio
import json
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


async def test_record_cleared_by_v1_loads_default_state() -> None:
    """A record that Crawlee v1 cleared by storing `None` loads the default state."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, b'', content_type='application/x-none')

    state = await _build_state().initialize()

    assert state == _State()


async def test_unparsable_record_raises_with_fix_note() -> None:
    """A record that isn't valid JSON makes `initialize` raise `ValueError` with a note to fix or delete it."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, '{not json', content_type='application/json')
    recoverable_state = _build_state()

    with pytest.raises(json.JSONDecodeError) as exc_info:
        await recoverable_state.initialize()

    assert any('Fix or delete the record' in note for note in exc_info.value.__notes__)


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


async def test_reset_store_without_persistence_does_nothing() -> None:
    """`reset_store` without persistence neither raises between `initialize` and `teardown` nor clears the record."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    recoverable_state = _build_state(persistence_enabled=False)
    await recoverable_state.initialize()

    await recoverable_state.reset_store()

    assert await kvs.get_value(KEY) == {'counter': 42, 'message': 'hello'}


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


async def test_persist_state_event_before_teardown_saves_nothing_after_it(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PERSIST_STATE event emitted right before `teardown` adds no save to the one `teardown` makes."""
    kvs = await KeyValueStore.open()
    set_value = AsyncMock(side_effect=kvs.set_value)
    monkeypatch.setattr(kvs, 'set_value', set_value)
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
    await recoverable_state.teardown()
    # Changed, so that a save after `teardown` wouldn't be skipped as unchanged.
    recoverable_state.current_value.counter = 1
    await event_manager.wait_for_all_listeners_to_complete()

    set_value.assert_awaited_once()


async def test_teardown_waits_for_save_in_progress(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`teardown` waits for a save that a PERSIST_STATE event started, so the save can't finish after it."""
    kvs = await KeyValueStore.open()
    set_value = kvs.set_value
    save_started = asyncio.Event()
    finish_save = asyncio.Event()

    async def slow_set_value(key: str, value: object, *, content_type: str | None = None) -> None:
        save_started.set()
        await finish_save.wait()
        await set_value(key, value, content_type=content_type)

    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=slow_set_value))
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
    await save_started.wait()
    monkeypatch.setattr(kvs, 'set_value', set_value)
    teardown_task = asyncio.create_task(recoverable_state.teardown())
    done, _ = await asyncio.wait([teardown_task], timeout=0.1)
    finish_save.set()
    await teardown_task

    assert not done


async def test_initialize_skips_wait_for_save_in_progress(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`initialize` on an initialized state doesn't wait for a save that a PERSIST_STATE event started."""
    kvs = await KeyValueStore.open()
    save_started = asyncio.Event()
    finish_save = asyncio.Event()

    async def slow_set_value(*_args: object, **_kwargs: object) -> None:
        save_started.set()
        await finish_save.wait()

    monkeypatch.setattr(kvs, 'set_value', AsyncMock(side_effect=slow_set_value))
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
    await save_started.wait()
    initialize_task = asyncio.create_task(recoverable_state.initialize())
    done, _ = await asyncio.wait([initialize_task], timeout=1)
    finish_save.set()
    await recoverable_state.teardown()

    assert initialize_task in done


async def test_unchanged_state_is_not_persisted_again(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PERSIST_STATE event writes the state only when it changed since it was last saved."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)

    set_value = AsyncMock(side_effect=kvs.set_value)
    monkeypatch.setattr(kvs, 'set_value', set_value)

    await _emit_persist_state(event_manager)
    assert set_value.await_count == 0

    recoverable_state.current_value.counter = 1
    await _emit_persist_state(event_manager)
    assert set_value.await_count == 1

    await recoverable_state.teardown()


async def test_teardown_writes_unchanged_state(event_manager: EventManager, monkeypatch: pytest.MonkeyPatch) -> None:
    """`teardown` writes the state even if it hasn't changed since it was last saved."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)

    set_value = AsyncMock(side_effect=kvs.set_value)
    monkeypatch.setattr(kvs, 'set_value', set_value)

    await recoverable_state.teardown()

    assert set_value.await_count == 1


async def test_debug_message_is_logged_only_for_writes(
    event_manager: EventManager, caplog: pytest.LogCaptureFixture
) -> None:
    """A PERSIST_STATE event logs the DEBUG message about persisting only when it writes the state."""
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        await _emit_persist_state(event_manager)
        recoverable_state.current_value.counter = 1
        await _emit_persist_state(event_manager)

    messages = [record.message for record in caplog.records if record.message.startswith('Persisting RecoverableState')]
    assert len(messages) == 1

    await recoverable_state.teardown()


async def test_failed_persist_is_retried_on_next_event(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PERSIST_STATE event after a failed write writes the state again, even though it hasn't changed."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    with monkeypatch.context() as failing_store:
        failing_store.setattr(kvs, 'set_value', AsyncMock(side_effect=RuntimeError('store is on fire')))
        await _emit_persist_state(event_manager)

    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 0, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_failed_write_that_landed_is_retried(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PERSIST_STATE event after a failed write that landed writes the state, even if it changed back since."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    set_value = kvs.set_value

    async def set_value_then_time_out(key: str, value: object, *, content_type: str | None = None) -> None:
        await set_value(key, value, content_type=content_type)
        raise TimeoutError

    recoverable_state.current_value.counter = 1
    with monkeypatch.context() as failing_store:
        failing_store.setattr(kvs, 'set_value', AsyncMock(side_effect=set_value_then_time_out))
        await _emit_persist_state(event_manager)

    recoverable_state.current_value.counter = 0
    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 0, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_change_during_write_is_persisted_by_next_event(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A change made while a write is in progress is written by the next PERSIST_STATE event."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    set_value = kvs.set_value

    async def set_value_then_change(key: str, value: object, *, content_type: str | None = None) -> None:
        await set_value(key, value, content_type=content_type)
        recoverable_state.current_value.counter = 1

    with monkeypatch.context() as changing_store:
        changing_store.setattr(kvs, 'set_value', AsyncMock(side_effect=set_value_then_change))
        await _emit_persist_state(event_manager)

    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 1, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_direct_persist_writes_unchanged_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """A direct `persist_state` call writes the state even if it hasn't changed."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await recoverable_state.persist_state()

    set_value = AsyncMock(side_effect=kvs.set_value)
    monkeypatch.setattr(kvs, 'set_value', set_value)

    await recoverable_state.persist_state()
    assert set_value.await_count == 1

    await recoverable_state.teardown()


async def test_loaded_state_is_not_written_back(event_manager: EventManager, monkeypatch: pytest.MonkeyPatch) -> None:
    """A state loaded from its record isn't written back until it changes."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 5, 'message': 'loaded'})
    recoverable_state = _build_state()
    await recoverable_state.initialize()

    set_value = AsyncMock(side_effect=kvs.set_value)
    monkeypatch.setattr(kvs, 'set_value', set_value)

    await _emit_persist_state(event_manager)
    assert set_value.await_count == 0

    await recoverable_state.teardown()


async def test_load_cancelled_after_read_keeps_earlier_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """A load cancelled after it read the record keeps the in-memory state established before it."""
    kvs = await KeyValueStore.open()
    await kvs.set_value(KEY, {'counter': 42, 'message': 'hello'})
    recoverable_state = _build_state()
    recoverable_state.reset()
    recoverable_state.current_value.counter = 7
    serialization_started = asyncio.Event()

    async def hanging_serialize_value(_value: object, _content_type: str | None) -> tuple[bytes, str]:
        serialization_started.set()
        await asyncio.sleep(3600)
        return b'', 'application/json'

    monkeypatch.setattr('crawlee.storages._utils.serialize_value', AsyncMock(side_effect=hanging_serialize_value))
    initialize_task = asyncio.create_task(recoverable_state.initialize())
    await asyncio.wait_for(serialization_started.wait(), timeout=5)
    initialize_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await initialize_task

    assert recoverable_state.current_value.counter == 7


async def test_reset_store_makes_next_persist_write(event_manager: EventManager) -> None:
    """After the record is cleared, the next persist writes the state even though it hasn't changed."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    await recoverable_state.teardown()

    await recoverable_state.reset_store()
    assert await kvs.get_value(KEY) is None

    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 0, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_failed_reset_store_makes_next_persist_write(
    event_manager: EventManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a failed `reset_store` that cleared the record, the next persist writes the unchanged state."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    await recoverable_state.teardown()
    set_value = kvs.set_value

    async def set_value_then_time_out(key: str, value: object, *, content_type: str | None = None) -> None:
        await set_value(key, value, content_type=content_type)
        raise TimeoutError

    with monkeypatch.context() as failing_store:
        failing_store.setattr(kvs, 'set_value', AsyncMock(side_effect=set_value_then_time_out))
        with pytest.raises(TimeoutError):
            await recoverable_state.reset_store()

    await recoverable_state.initialize()
    await _emit_persist_state(event_manager)
    assert await kvs.get_value(KEY) == {'counter': 0, 'message': 'hello'}

    await recoverable_state.teardown()


async def test_persisted_record_matches_json_value_format() -> None:
    """The state is written as the same bytes as an equal JSON value set in the store."""
    kvs = await KeyValueStore.open()
    recoverable_state = _build_state()
    await recoverable_state.initialize()
    await recoverable_state.persist_state()
    await kvs.set_value('json-value', {'counter': 0, 'message': 'hello'})

    state_record = await kvs.get_record(KEY)
    json_record = await kvs.get_record('json-value')

    assert state_record is not None
    assert json_record is not None
    assert state_record.value == json_record.value

    await recoverable_state.teardown()


async def test_infinite_float_is_restored() -> None:
    """A state holding an infinite float is restored with it by a new instance."""

    class _FloatState(BaseModel):
        value: float = 0

    recoverable_state = RecoverableState(
        default_state=_FloatState(), persist_state_key=KEY, persistence_enabled=True, logger=logger
    )
    await recoverable_state.initialize()
    recoverable_state.current_value.value = float('inf')
    await recoverable_state.teardown()

    restored_state = RecoverableState(
        default_state=_FloatState(), persist_state_key=KEY, persistence_enabled=True, logger=logger
    )

    assert (await restored_state.initialize()).value == float('inf')
    await restored_state.teardown()
