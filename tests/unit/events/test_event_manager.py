from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import timedelta
from functools import update_wrapper
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee.events import Event, EventManager, EventSystemInfoData

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as event_manager:
        yield event_manager


@pytest.fixture
def event_system_info_data() -> EventSystemInfoData:
    return MagicMock(spec=EventSystemInfoData)


@pytest.fixture
def async_listener() -> AsyncMock:
    async def async_listener(payload: Any) -> None:
        pass

    al = AsyncMock()
    update_wrapper(al, async_listener)
    return al


@pytest.fixture
def sync_listener() -> MagicMock:
    def sync_listener(payload: Any) -> None:
        pass

    sl = MagicMock()
    update_wrapper(sl, sync_listener)
    return sl


async def test_emit_invokes_registered_sync_listener(
    sync_listener: MagicMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()

    assert sync_listener.call_count == 1
    assert sync_listener.call_args[0] == (event_system_info_data,)


async def test_emit_invokes_both_sync_and_async_listeners(
    sync_listener: MagicMock,
    async_listener: AsyncMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()

    assert async_listener.call_count == 1
    assert async_listener.call_args[0] == (event_system_info_data,)

    assert sync_listener.call_count == 1
    assert sync_listener.call_args[0] == (event_system_info_data,)


async def test_emit_event_with_no_listeners(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
    async_listener: AsyncMock,
) -> None:
    # Register a listener for a different event
    event_manager.on(event=Event.ABORTING, listener=async_listener)

    # Attempt to emit an event for which no listeners are registered, it should not fail
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    # Ensure the listener for the other event was not called
    assert async_listener.call_count == 0


async def test_emit_invokes_parameterless_listener(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    sync_mock = MagicMock()

    def sync_listener() -> None:
        sync_mock()

    async_mock = MagicMock()

    async def async_listener() -> None:
        async_mock()

    event_manager.on(event=Event.SYSTEM_INFO, listener=sync_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
    await event_manager.wait_for_all_listeners_to_complete()

    assert sync_mock.call_count == 1
    assert async_mock.call_count == 1


async def test_remove_nonexistent_listener_does_not_fail(
    async_listener: AsyncMock,
    event_manager: EventManager,
) -> None:
    # Attempt to remove a specific listener that was never added.
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)
    # Attempt to remove all listeners.
    event_manager.off(event=Event.ABORTING)


async def test_removed_listener_not_invoked_on_emit(
    async_listener: AsyncMock,
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await event_manager.wait_for_all_listeners_to_complete()
    assert async_listener.call_count == 0


async def test_close_clears_listeners_and_tasks(async_listener: AsyncMock) -> None:
    async with EventManager() as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)

    assert async_listener.call_count == 0
    assert len(event_manager._listener_tasks) == 0
    assert len(event_manager._listeners_to_wrappers) == 0


async def test_close_after_emit_processes_event(
    async_listener: AsyncMock,
    event_system_info_data: EventSystemInfoData,
) -> None:
    async with EventManager() as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    # Event should be processed before the event manager is closed
    assert async_listener.call_count == 1
    assert async_listener.call_args[0] == (event_system_info_data,)

    assert len(event_manager._listener_tasks) == 0
    assert len(event_manager._listeners_to_wrappers) == 0


async def test_wait_for_all_listeners_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Simulate long-running listener tasks
    async def long_running_listener() -> None:
        await asyncio.sleep(10)

    # Define a side effect function that raises CancelledError
    async def mock_async_wait(*_: Any, **__: Any) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError), caplog.at_level(logging.WARNING):  # noqa: PT012
        async with EventManager(close_timeout=timedelta(milliseconds=10)) as event_manager:
            event_manager.on(event=Event.SYSTEM_INFO, listener=long_running_listener)

            # Use monkeypatch to replace asyncio.wait with mock_async_wait
            monkeypatch.setattr('asyncio.wait', mock_async_wait)


async def test_methods_raise_error_when_not_active(event_system_info_data: EventSystemInfoData) -> None:
    event_manager = EventManager()

    assert event_manager.active is False

    with pytest.raises(RuntimeError, match=r'EventManager is not active.'):
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    with pytest.raises(RuntimeError, match=r'EventManager is not active.'):
        await event_manager.wait_for_all_listeners_to_complete()

    async with event_manager:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
        await event_manager.wait_for_all_listeners_to_complete()

        assert event_manager.active is True


async def test_wait_for_all_listeners_from_within_a_listener_does_not_deadlock(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Waiting from within a listener must not self-await, yet must still await the other listeners."""
    other_listener_done = asyncio.Event()
    waiter_done = asyncio.Event()
    other_done_when_wait_returned: bool | None = None

    async def other_listener(_: Any) -> None:
        await asyncio.sleep(0.2)
        other_listener_done.set()

    async def waiting_listener(_: Any) -> None:
        nonlocal other_done_when_wait_returned
        await event_manager.wait_for_all_listeners_to_complete()
        other_done_when_wait_returned = other_listener_done.is_set()
        waiter_done.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=waiting_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await asyncio.wait_for(waiter_done.wait(), timeout=5)

    # No self-await deadlock, and the wait must have blocked until the co-registered listener finished.
    assert other_done_when_wait_returned is True
    assert other_listener_done.is_set()


async def test_wait_from_within_multiple_listeners_does_not_deadlock(
    event_manager: EventManager,
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Several listeners each waiting for all listeners at once must not deadlock one another."""
    first_done = asyncio.Event()
    second_done = asyncio.Event()

    async def first_waiting_listener(_: Any) -> None:
        await event_manager.wait_for_all_listeners_to_complete()
        first_done.set()

    async def second_waiting_listener(_: Any) -> None:
        await event_manager.wait_for_all_listeners_to_complete()
        second_done.set()

    event_manager.on(event=Event.SYSTEM_INFO, listener=first_waiting_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=second_waiting_listener)
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    await asyncio.wait_for(asyncio.gather(first_done.wait(), second_done.wait()), timeout=5)

    assert first_done.is_set()
    assert second_done.is_set()


async def test_close_from_within_a_listener_does_not_deadlock_or_error(
    event_system_info_data: EventSystemInfoData,
) -> None:
    """Closing the event manager from within a listener (as `Actor.exit()` does) must not deadlock or raise."""
    event_manager = EventManager()
    await event_manager.__aenter__()

    # A wrapper that finalizes after close raises through pyee onto the event loop (its `error` listener is
    # gone by then, removed during close), so watch both channels to catch a stray exception on finalize.
    emitter_errors: list[BaseException] = []
    event_manager._event_emitter.add_listener('error', emitter_errors.append)
    loop_errors: list[dict[str, Any]] = []
    asyncio.get_running_loop().set_exception_handler(lambda _loop, context: loop_errors.append(context))

    closed = asyncio.Event()
    other_listener_done = asyncio.Event()

    async def other_listener(_: Any) -> None:
        await asyncio.sleep(0.2)
        other_listener_done.set()

    async def closing_listener(_: Any) -> None:
        await event_manager.__aexit__(None, None, None)
        closed.set()

    # Register a regular listener too, so closing must await a concurrently-running listener (the real
    # `Actor.exit()` shape) and exercise the task-set cleanup while another listener is still in flight.
    event_manager.on(event=Event.SYSTEM_INFO, listener=other_listener)
    event_manager.on(event=Event.SYSTEM_INFO, listener=closing_listener)

    tasks_before = asyncio.all_tasks()
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    try:
        await asyncio.wait_for(closed.wait(), timeout=5)
        # Deterministically drain the listener-wrapper tasks so their `finally` blocks (and any exception
        # they raise on finalize) run before we assert - no arbitrary sleep.
        spawned = asyncio.all_tasks() - tasks_before - {asyncio.current_task()}
        if spawned:
            await asyncio.wait(spawned)
    finally:
        # On a regression the listener may deadlock; cap the cleanup so the primary failure surfaces instead
        # of hanging indefinitely.
        if event_manager.active:
            with suppress(Exception):
                await asyncio.wait_for(event_manager.__aexit__(None, None, None), timeout=5)

    # The discard-not-remove fix means no wrapper raises on finalize; the `remove` regression would surface
    # on one of these channels (whichever fires depends on whether the `error` listener is still registered).
    assert emitter_errors == []
    assert loop_errors == []
    assert other_listener_done.is_set()
    assert event_manager.active is False
    assert len(event_manager._listener_tasks) == 0


async def test_event_manager_in_context_persistence() -> None:
    """Test that entering the `EventManager` context emits persist state event at least once."""
    event_manager = EventManager()

    with mock.patch.object(event_manager, '_emit_persist_state_event', AsyncMock()) as mocked_emit_persist_state_event:
        async with event_manager:
            pass

    assert mocked_emit_persist_state_event.call_count >= 1
