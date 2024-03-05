from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from crawlee.autoscaling import Snapshotter, SystemStatus
from crawlee.autoscaling.types import CpuSnapshot, LoadRatioInfo
from crawlee.events import LocalEventManager


@pytest_asyncio.fixture()
async def snapshotter() -> AsyncGenerator[Snapshotter, None]:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager)
        await snapshotter.start()
        yield snapshotter
        await snapshotter.stop()


@pytest.fixture()
def now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.mark.asyncio()
async def test_start_stop_lifecycle() -> None:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager)
        await snapshotter.start()

        system_status = SystemStatus(snapshotter)
        system_status.get_current_status()
        system_status.get_historical_status()

        await snapshotter.stop()


def test_cpu_is_overloaded(snapshotter: Snapshotter, now: datetime) -> None:
    system_status = SystemStatus(snapshotter, max_cpu_overloaded_ratio=0.5)
    system_status._snapshotter._cpu_snapshots = [
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.75, created_at=now - timedelta(minutes=4)),
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.75, created_at=now - timedelta(minutes=3)),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.75, created_at=now - timedelta(minutes=2)),
        CpuSnapshot(used_ratio=0.9, max_used_ratio=0.75, created_at=now - timedelta(minutes=1)),
    ]
    cpu_info = system_status._is_cpu_overloaded()

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.667)
    assert cpu_info.is_overloaded is True


def test_cpu_is_not_overloaded(snapshotter: Snapshotter, now: datetime) -> None:
    system_status = SystemStatus(snapshotter, max_cpu_overloaded_ratio=0.5)
    system_status._snapshotter._cpu_snapshots = [
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.75, created_at=now - timedelta(minutes=4)),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.75, created_at=now - timedelta(minutes=3)),
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.75, created_at=now - timedelta(minutes=2)),
        CpuSnapshot(used_ratio=0.5, max_used_ratio=0.75, created_at=now - timedelta(minutes=1)),
    ]
    cpu_info = system_status._is_cpu_overloaded()

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.333)
    assert cpu_info.is_overloaded is False
