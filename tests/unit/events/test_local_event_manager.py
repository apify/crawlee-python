from __future__ import annotations

import asyncio
import threading
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from crawlee._utils.system import CpuInfo, MemoryInfo
from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    import proclimits
    import pytest


async def test_emit_system_info_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """The recurring task emits the first `SystemInfo` event as soon as it starts, without waiting for the interval."""
    # Both readings are replaced with instant ones - a real `get_cpu_info` may sample the CPU utilization over 100 ms,
    # and on a loaded runner it takes far longer than that.
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda _: MagicMock(spec=CpuInfo))
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


async def test_lazy_start_emits_system_info(monkeypatch: pytest.MonkeyPatch) -> None:
    """A manager started by `on`, without `async with`, emits `SystemInfo` events too."""
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda _: MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))
    received = asyncio.Event()

    async def listener(_event_data: EventSystemInfoData) -> None:
        received.set()

    # An interval this long means the event can only come from the immediate first run of the recurring task.
    event_manager = LocalEventManager(system_info_interval=timedelta(hours=1))
    event_manager.on(event=Event.SYSTEM_INFO, listener=listener)

    await asyncio.wait_for(received.wait(), timeout=5)


async def test_system_info_readings_run_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both readings block their thread, so they have to run at the same time - the barrier clears only if they do."""
    # A party left waiting alone breaks the barrier, which fails the test instead of hanging it.
    barrier = threading.Barrier(2, timeout=5)

    def get_cpu_info_at_barrier(_cpu_load: Any) -> Any:
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


async def test_cpu_sampler_restarts_with_each_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each session samples the CPU afresh, instead of measuring across the idle gap since the previous one."""
    samplers: list[proclimits.CpuLoad] = []

    def get_cpu_info(cpu_load: proclimits.CpuLoad) -> Any:
        samplers.append(cpu_load)
        return MagicMock(spec=CpuInfo)

    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', get_cpu_info)
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))

    event_manager = LocalEventManager(system_info_interval=timedelta(hours=1))

    for _ in range(2):
        async with event_manager:
            received = asyncio.Event()

            async def listener(_event_data: EventSystemInfoData, received: asyncio.Event = received) -> None:
                received.set()

            event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
            await asyncio.wait_for(received.wait(), timeout=5)

    assert len(samplers) == 2
    assert samplers[0] is not samplers[1]


async def test_cpu_sampler_restarts_after_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A listener registered after a gap without listeners gets a fresh CPU sampler, not one spanning the gap."""
    get_cpu_info = MagicMock(return_value=MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', get_cpu_info)
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))
    listener = AsyncMock()

    async with LocalEventManager(system_info_interval=timedelta(hours=1)) as event_manager:
        # Only the direct emissions below sample the CPU. The recurring one is cancelled before it ever runs.
        await event_manager._emit_system_info_event_rec_task.stop()

        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await event_manager._emit_system_info_event()

        event_manager.off(event=Event.SYSTEM_INFO, listener=listener)
        await event_manager._emit_system_info_event()

        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await event_manager._emit_system_info_event()

    samplers = [call.args[0] for call in get_cpu_info.call_args_list]
    assert len(samplers) == 2
    assert samplers[0] is not samplers[1]


async def test_system_info_only_when_listened(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a `SystemInfo` listener, the manager skips the readings."""
    get_cpu_info = MagicMock(return_value=MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', get_cpu_info)
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))

    event_manager = LocalEventManager(system_info_interval=timedelta(hours=1))
    listener = AsyncMock()

    async with event_manager:
        # Only the direct emissions below sample the CPU. The recurring one is cancelled before it ever runs.
        await event_manager._emit_system_info_event_rec_task.stop()

        await event_manager._emit_system_info_event()
        assert get_cpu_info.call_count == 0

        event_manager.on(event=Event.SYSTEM_INFO, listener=listener)
        await event_manager._emit_system_info_event()
        await event_manager.wait_for_all_listeners_to_complete()

    assert get_cpu_info.call_count == 1
    assert listener.call_count == 1


async def test_system_info_uses_emit(monkeypatch: pytest.MonkeyPatch) -> None:
    """The `SystemInfo` event goes through `emit`, so a subclass that overrides it sees the event."""
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda _: MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))

    async with LocalEventManager(system_info_interval=timedelta(hours=1)) as event_manager:
        # Only the direct emission below calls `emit`. The recurring one is cancelled before it ever runs.
        await event_manager._emit_system_info_event_rec_task.stop()
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())

        with patch.object(event_manager, 'emit', wraps=event_manager.emit) as emit:
            await event_manager._emit_system_info_event()

    emit.assert_called_once()
    assert emit.call_args.kwargs['event'] == Event.SYSTEM_INFO


async def test_on_during_exit_keeps_system_info(monkeypatch: pytest.MonkeyPatch) -> None:
    """A `SystemInfo` listener registered while the owner's exit waits for the listeners keeps getting the events."""
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda _: MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))
    received = asyncio.Event()

    async def system_info_listener(_event_data: EventSystemInfoData) -> None:
        received.set()

    # An interval this long means the event can only come from the immediate first run of the restarted task.
    event_manager = LocalEventManager(system_info_interval=timedelta(hours=1))

    async def register_listener() -> None:
        event_manager.off(event=Event.PERSIST_STATE, listener=register_listener)
        event_manager.on(event=Event.SYSTEM_INFO, listener=system_info_listener)

    async with event_manager:
        event_manager.on(event=Event.PERSIST_STATE, listener=register_listener)

    await asyncio.wait_for(received.wait(), timeout=5)


async def test_on_as_exit_begins_keeps_system_info(monkeypatch: pytest.MonkeyPatch) -> None:
    """A `SystemInfo` listener registered at the first await of the owner's exit keeps getting the events."""
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', lambda _: MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))
    received = asyncio.Event()

    async def system_info_listener(_event_data: EventSystemInfoData) -> None:
        received.set()

    # An interval this long means the event can only come from the immediate first run of the restarted task.
    event_manager = LocalEventManager(system_info_interval=timedelta(hours=1))

    async def register_listener() -> None:
        event_manager.on(event=Event.SYSTEM_INFO, listener=system_info_listener)

    async with event_manager:
        # Scheduled now, so it runs as soon as the exit yields for the first time.
        register_task = asyncio.create_task(register_listener())

    await register_task
    await asyncio.wait_for(received.wait(), timeout=5)


async def test_exit_stops_system_info_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exit stops the `SystemInfo` readings before it waits for the listeners of the final `PersistState` event."""
    get_cpu_info = MagicMock(return_value=MagicMock(spec=CpuInfo))
    monkeypatch.setattr('crawlee.events._local_event_manager.get_cpu_info', get_cpu_info)
    monkeypatch.setattr('crawlee.events._local_event_manager.get_memory_info', lambda: MagicMock(spec=MemoryInfo))
    readings_during_save: list[int] = []

    async def slow_save() -> None:
        # A reading that its thread already started when the exit began may still finish meanwhile.
        await asyncio.sleep(0.05)
        readings_before = get_cpu_info.call_count
        await asyncio.sleep(0.1)
        readings_during_save.append(get_cpu_info.call_count - readings_before)

    async with LocalEventManager(system_info_interval=timedelta(milliseconds=10)) as event_manager:
        event_manager.on(event=Event.SYSTEM_INFO, listener=AsyncMock())
        event_manager.on(event=Event.PERSIST_STATE, listener=slow_save)

    assert readings_during_save == [0]
