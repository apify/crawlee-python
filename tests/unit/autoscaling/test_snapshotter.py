from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from crawlee.autoscaling import Snapshotter
from crawlee.autoscaling.types import CpuSnapshot
from crawlee.events import EventManager, LocalEventManager


@pytest.fixture()
def snapshotter() -> Snapshotter:
    mocked_event_manager = AsyncMock(spec=EventManager)
    return Snapshotter(event_manager=mocked_event_manager)


@pytest.mark.asyncio()
async def test_start_stop() -> None:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager=event_manager)
        await snapshotter.start()
        await snapshotter.stop()


def test_get_cpu_sample(snapshotter: Snapshotter) -> None:
    now = datetime.now(tz=timezone.utc)
    cpu_snapshots = [
        CpuSnapshot(
            used_ratio=0.5,
            max_used_ratio=0.95,
            created_at=now - timedelta(hours=delta),
        )
        for delta in range(5, 0, -1)
    ]

    snapshotter._cpu_snapshots = cpu_snapshots  # type: ignore

    # When no sample duration is provided it should return all snapshots
    samples = snapshotter.get_cpu_sample()
    assert len(samples) == len(cpu_snapshots)

    duration = timedelta(hours=0.5)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == 1

    duration = timedelta(hours=2.5)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == 3

    duration = timedelta(hours=10)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == len(cpu_snapshots)


def test_get_samples_empty(snapshotter: Snapshotter) -> None:
    # All get resource sample uses the same helper function, so testing only one of them properly (CPU) should be
    # enough. Here just call all of them returning empty list to make sure they don't crash.
    assert snapshotter.get_cpu_sample() == []
    assert snapshotter.get_memory_sample() == []
    assert snapshotter.get_event_loop_sample() == []
    assert snapshotter.get_client_sample() == []


#
