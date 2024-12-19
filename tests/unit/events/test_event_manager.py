from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from functools import update_wrapper
from typing import TYPE_CHECKING, Any
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

    await asyncio.sleep(0.1)  # Allow some time for the event to be processed

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

    await asyncio.sleep(0.1)  # Allow some time for the event to be processed

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
    await asyncio.sleep(0.1)  # Allow some time for the event to be processed

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
    await asyncio.sleep(0.1)  # Allow some time for the event to be processed

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

    await asyncio.sleep(0.1)  # Allow some time for the event to be processed
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

    with pytest.raises(RuntimeError, match='EventManager is not active.'):
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)

    with pytest.raises(RuntimeError, match='EventManager is not active.'):
        await event_manager.wait_for_all_listeners_to_complete()

    with pytest.raises(RuntimeError, match='EventManager is already active.'):
        async with event_manager, event_manager:
            pass

    async with event_manager:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_info_data)
        await event_manager.wait_for_all_listeners_to_complete()

        assert event_manager.active is True
