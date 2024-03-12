from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest

from crawlee.autoscaling import Snapshotter, SystemStatus
from crawlee.autoscaling.types import (
    ClientSnapshot,
    CpuSnapshot,
    EventLoopSnapshot,
    LoadRatioInfo,
    MemorySnapshot,
    SystemInfo,
)
from crawlee.events import LocalEventManager


@pytest.fixture()
async def snapshotter() -> AsyncGenerator[Snapshotter, None]:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager)
        await snapshotter.start()
        yield snapshotter
        await snapshotter.stop()


@pytest.fixture()
def now() -> datetime:
    return datetime.now(timezone.utc)


async def test_start_stop_lifecycle() -> None:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager)
        await snapshotter.start()

        system_status = SystemStatus(snapshotter)
        system_status.get_current_status()
        system_status.get_historical_status()

        await snapshotter.stop()


def test_cpu_is_overloaded(snapshotter: Snapshotter, now: datetime) -> None:
    system_status = SystemStatus(snapshotter, cpu_overload_threshold=0.5)
    system_status._snapshotter._cpu_snapshots = [
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.75, created_at=now - timedelta(minutes=3)),
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.75, created_at=now - timedelta(minutes=2)),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.75, created_at=now - timedelta(minutes=1)),
        CpuSnapshot(used_ratio=0.9, max_used_ratio=0.75, created_at=now),
    ]
    cpu_info = system_status._is_cpu_overloaded()

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.667)
    assert cpu_info.is_overloaded is True


def test_cpu_is_not_overloaded(snapshotter: Snapshotter, now: datetime) -> None:
    system_status = SystemStatus(snapshotter, cpu_overload_threshold=0.5)
    system_status._snapshotter._cpu_snapshots = [
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.75, created_at=now - timedelta(minutes=3)),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.75, created_at=now - timedelta(minutes=2)),
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.75, created_at=now - timedelta(minutes=1)),
        CpuSnapshot(used_ratio=0.5, max_used_ratio=0.75, created_at=now),
    ]
    cpu_info = system_status._is_cpu_overloaded()

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.333)
    assert cpu_info.is_overloaded is False


def test_get_system_info(snapshotter: Snapshotter, now: datetime) -> None:
    system_status = SystemStatus(
        snapshotter,
        current_history=timedelta(minutes=1),
        cpu_overload_threshold=0.5,
        memory_overload_threshold=0.5,
        event_loop_overload_threshold=0.5,
        client_overload_threshold=0.5,
    )

    # Add CPU snapshots
    system_status._snapshotter._cpu_snapshots = [
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.75, created_at=now - timedelta(minutes=3)),
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.75, created_at=now - timedelta(minutes=2)),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.75, created_at=now - timedelta(minutes=1)),
        CpuSnapshot(used_ratio=0.9, max_used_ratio=0.75, created_at=now),
    ]

    # Add memory snapshots
    system_status._snapshotter._memory_snapshots = [
        MemorySnapshot(
            total_bytes=16 * 1024**3,
            current_bytes=4 * 1024**3,
            max_memory_bytes=12 * 1024**3,
            max_used_memory_ratio=0.8,
            created_at=now - timedelta(minutes=3),
        ),
        MemorySnapshot(
            total_bytes=8 * 1024**3,
            current_bytes=7 * 1024**3,
            max_memory_bytes=8 * 1024**3,
            max_used_memory_ratio=0.8,
            created_at=now - timedelta(minutes=2),
        ),
        MemorySnapshot(
            total_bytes=32 * 1024**3,
            current_bytes=28 * 1024**3,
            max_memory_bytes=30 * 1024**3,
            max_used_memory_ratio=0.8,
            created_at=now - timedelta(minutes=1),
        ),
        MemorySnapshot(
            total_bytes=64 * 1024**3,
            current_bytes=48 * 1024**3,
            max_memory_bytes=60 * 1024**3,
            max_used_memory_ratio=0.8,
            created_at=now,
        ),
    ]

    # Add event loop snapshots
    system_status._snapshotter._event_loop_snapshots = [
        EventLoopSnapshot(
            delay=timedelta(milliseconds=700),
            max_delay=timedelta(milliseconds=500),
            created_at=now - timedelta(minutes=3),
        ),
        EventLoopSnapshot(
            delay=timedelta(milliseconds=600),
            max_delay=timedelta(milliseconds=500),
            created_at=now - timedelta(minutes=2),
        ),
        EventLoopSnapshot(
            delay=timedelta(milliseconds=200),
            max_delay=timedelta(milliseconds=500),
            created_at=now - timedelta(minutes=1),
        ),
        EventLoopSnapshot(
            delay=timedelta(milliseconds=100),
            max_delay=timedelta(milliseconds=500),
            created_at=now,
        ),
    ]

    # Add client snapshots
    system_status._snapshotter._client_snapshots = [
        ClientSnapshot(num_of_errors=1, max_num_of_errors=2, created_at=now - timedelta(minutes=3)),
        ClientSnapshot(num_of_errors=1, max_num_of_errors=2, created_at=now - timedelta(minutes=2)),
        ClientSnapshot(num_of_errors=2, max_num_of_errors=2, created_at=now - timedelta(minutes=1)),
        ClientSnapshot(num_of_errors=0, max_num_of_errors=2, created_at=now),
    ]

    # Test current system info
    current_system_info = system_status.get_current_status()
    assert current_system_info == SystemInfo(
        cpu_info=LoadRatioInfo(limit_ratio=system_status._cpu_overload_threshold, actual_ratio=1.0),
        memory_info=LoadRatioInfo(limit_ratio=system_status._memory_overload_threshold, actual_ratio=1.0),
        event_loop_info=LoadRatioInfo(limit_ratio=system_status._event_loop_overload_threshold, actual_ratio=0),
        client_info=LoadRatioInfo(limit_ratio=system_status._client_overload_threshold, actual_ratio=0),
        created_at=current_system_info.created_at,
    )
    assert current_system_info.is_system_idle is False

    # Test historical system info
    historical_system_info = system_status.get_historical_status()
    assert historical_system_info == SystemInfo(
        cpu_info=LoadRatioInfo(limit_ratio=system_status._cpu_overload_threshold, actual_ratio=0.667),
        memory_info=LoadRatioInfo(limit_ratio=system_status._memory_overload_threshold, actual_ratio=0.667),
        event_loop_info=LoadRatioInfo(limit_ratio=system_status._event_loop_overload_threshold, actual_ratio=0.333),
        client_info=LoadRatioInfo(limit_ratio=system_status._client_overload_threshold, actual_ratio=0),
        created_at=historical_system_info.created_at,
    )
    assert historical_system_info.is_system_idle is False
