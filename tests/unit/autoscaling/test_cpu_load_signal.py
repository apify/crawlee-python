from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import CpuInfo, MemoryInfo
from crawlee.autoscaling import CpuLoadSignal, LoadSignalStartContext
from crawlee.configuration import Configuration
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from crawlee.events import LocalEventManager


def start_context(configuration: Configuration | None = None) -> LoadSignalStartContext:
    """Build the context a signal starts with, sampled over 30 seconds."""
    return LoadSignalStartContext(
        max_sample_window=timedelta(seconds=30), configuration=configuration or Configuration()
    )


def system_info(used_ratio: float) -> EventSystemInfoData:
    """Build a `SYSTEM_INFO` payload reporting the given CPU usage."""
    return EventSystemInfoData(
        cpu_info=CpuInfo(used_ratio=used_ratio),
        memory_info=MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_gb(1),
            system_wide_used_size=ByteSize.from_gb(2),
        ),
    )


async def test_cpu_overloaded_above_limit(event_manager: LocalEventManager) -> None:
    """The CPU counts as overloaded while its usage is above `max_used_cpu_ratio`."""
    signal = CpuLoadSignal()
    await signal.start(start_context(Configuration(max_used_cpu_ratio=0.5)))

    for used_ratio in [0.4, 0.6]:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=system_info(used_ratio))
        await event_manager.wait_for_all_listeners_to_complete()

    await signal.stop()

    sample = signal.get_sample()
    assert [snapshot.used_ratio for snapshot in sample] == [0.4, 0.6]
    assert [snapshot.is_overloaded for snapshot in sample] == [False, True]


async def test_cpu_stop_unsubscribes(event_manager: LocalEventManager) -> None:
    """A stopped signal records no further snapshots."""
    signal = CpuLoadSignal()
    await signal.start(start_context())
    await signal.stop()

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=system_info(0.5))
    await event_manager.wait_for_all_listeners_to_complete()

    assert signal.get_sample() == []


async def test_cpu_restart_drops_snapshots(event_manager: LocalEventManager) -> None:
    """A restarted signal is not judged on measurements from before the downtime."""
    signal = CpuLoadSignal()
    await signal.start(start_context())

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=system_info(0.5))
    await event_manager.wait_for_all_listeners_to_complete()
    await signal.stop()

    await signal.start(start_context())
    await signal.stop()

    assert signal.get_sample() == []
