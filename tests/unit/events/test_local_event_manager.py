from __future__ import annotations

import asyncio
import threading
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

from crawlee._utils.system import CpuInfo, MemoryInfo
from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    import pytest


async def test_emit_system_info_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """The recurring task emits the first `SystemInfo` event as soon as it starts, without waiting for the interval."""
    # Both readings are replaced with instant ones - a real `get_cpu_info` samples the CPU utilization over 100 ms,
    # and on a loaded runner it takes far longer than that.
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda: MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))

    mocked_listener = AsyncMock()
    received = asyncio.Event()

    async def async_listener(payload: Any) -> None:
        await mocked_listener(payload)
        received.set()

    # An interval this long means the event can only come from the immediate first run of the recurring task.
    async with LocalEventManager(system_info_interval=timedelta(hours=1)) as event_manager:
        # Registered before anything yields to the event loop, so the very first emission already reaches it.
        event_manager.on(event=Event.SYSTEM_INFO, listener=async_listener)
        await asyncio.wait_for(received.wait(), timeout=5)

    assert mocked_listener.call_count >= 1
    assert isinstance(mocked_listener.call_args[0][0], EventSystemInfoData)


async def test_system_info_readings_run_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both readings block their thread, so they have to run at the same time - the barrier clears only if they do."""
    # A party left waiting alone breaks the barrier, which fails the test instead of hanging it.
    barrier = threading.Barrier(2, timeout=5)

    def get_cpu_info_at_barrier() -> Any:
        barrier.wait()
        return MagicMock(spec=CpuInfo)

    def get_memory_info_at_barrier() -> Any:
        barrier.wait()
        return MagicMock(spec=MemoryInfo)

    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', get_cpu_info_at_barrier)
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', get_memory_info_at_barrier)

    received: list[EventSystemInfoData] = []

    async def listener(event_data: EventSystemInfoData) -> None:
        received.append(event_data)

    async with LocalEventManager() as event_manager:
        # A recurring emission would pair up with the direct one below at the barrier, so stop it first. It is
        # cancelled before it ever runs, as nothing has yielded to the event loop since it was started.
        await event_manager._emit_system_info_event_rec_task.stop()

        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await event_manager._emit_system_info_event()
        await event_manager.wait_for_all_listeners_to_complete()

    assert len(received) == 1
