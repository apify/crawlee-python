from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee.events.event_manager import EventManager
from crawlee.events.types import Event, EventSystemInfoData


@pytest.fixture()
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as event_manager:
        yield event_manager


@pytest.fixture()
def event_system_info_data() -> EventSystemInfoData:
    return MagicMock(spec=EventSystemInfoData)


@pytest.fixture()
def async_listener() -> AsyncMock:
    al = AsyncMock()
    al.__name__ = 'async_listener'  # To avoid issues with the function name
    return al


@pytest.fixture()
def sync_listener() -> MagicMock:
    sl = MagicMock()
    sl.__name__ = 'sync_listener'  # To avoid issues with the function name
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

    # Ensure the listener for the other event was not called
    assert async_listener.call_count == 0


async def test_remove_nonexistent_listener_does_not_fail(
    async_listener: AsyncMock,
    event_manager: EventManager,
) -> None:
    # Attempt to remove a listener that was never added.
    event_manager.off(event=Event.SYSTEM_INFO, listener=async_listener)


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
