from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee.autoscaling import Snapshotter
from crawlee.autoscaling.types import CpuSnapshot, Snapshot
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

    snapshotter._cpu_snapshots = cpu_snapshots

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


def test_snapshot_pruning(snapshotter: Snapshotter) -> None:
    # Set the snapshot history to 2 hours
    snapshotter._snapshot_history = timedelta(hours=2)

    # Create timestamps for testing
    now = datetime.now(tz=timezone.utc)
    two_hours_ago = now - timedelta(hours=2)
    three_hours_ago = now - timedelta(hours=3)
    five_hours_ago = now - timedelta(hours=5)

    # Create mock snapshots with varying creation times
    snapshots = [
        CpuSnapshot(used_ratio=0.5, max_used_ratio=0.95, created_at=five_hours_ago),
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.95, created_at=three_hours_ago),
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.95, created_at=two_hours_ago),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.95, created_at=now),
    ]

    # Assign these snapshots to one of the lists (e.g., CPU snapshots)
    snapshotter._cpu_snapshots = snapshots

    # Prune snapshots older than 2 hours
    snapshots_casted = cast(list[Snapshot], snapshotter._cpu_snapshots)
    snapshotter._prune_snapshots(snapshots_casted, now)

    # Check that only the last two snapshots remain
    assert len(snapshotter._cpu_snapshots) == 2
    assert snapshotter._cpu_snapshots[0].created_at == two_hours_ago
    assert snapshotter._cpu_snapshots[1].created_at == now


def test_snapshot_pruning_empty(snapshotter: Snapshotter) -> None:
    now = datetime.now(tz=timezone.utc)
    snapshotter._cpu_snapshots = []
    snapshots_casted = cast(list[Snapshot], snapshotter._cpu_snapshots)
    snapshotter._prune_snapshots(snapshots_casted, now)
    assert snapshotter._cpu_snapshots == []


def test_snapshot_pruning_no_prune(snapshotter: Snapshotter) -> None:
    snapshotter._snapshot_history = timedelta(hours=2)

    # Create timestamps for testing
    now = datetime.now(tz=timezone.utc)
    one_hour_ago = now - timedelta(hours=1)

    # Create mock snapshots with varying creation times
    snapshots = [
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.95, created_at=one_hour_ago),
        CpuSnapshot(used_ratio=0.8, max_used_ratio=0.95, created_at=now),
    ]

    # Assign these snapshots to one of the lists (e.g., CPU snapshots)
    snapshotter._cpu_snapshots = snapshots

    # Prune snapshots older than 2 hours
    snapshots_casted = cast(list[Snapshot], snapshotter._cpu_snapshots)
    snapshotter._prune_snapshots(snapshots_casted, now)

    # Check that only the last two snapshots remain
    assert len(snapshotter._cpu_snapshots) == 2
    assert snapshotter._cpu_snapshots[0].created_at == one_hour_ago
    assert snapshotter._cpu_snapshots[1].created_at == now


def test_evaluate_memory_load_high(monkeypatch: pytest.MonkeyPatch, snapshotter: Snapshotter) -> None:
    mock_logger_warn = MagicMock()
    monkeypatch.setattr(getLogger('crawlee.autoscaling.snapshotter'), 'warning', mock_logger_warn)
    snapshotter._max_memory_bytes = 8 * 1024**3  # 8 GB

    high_memory_usage = int(0.9 * 8 * 1024**3)  # 90% of 8 GB

    snapshotter._evaluate_memory_load(
        current_memory_usage_bytes=high_memory_usage,
        snapshot_timestamp=datetime.now(tz=timezone.utc),
    )

    assert mock_logger_warn.call_count == 1
    assert 'Memory is critically overloaded' in mock_logger_warn.call_args[0][0]

    # It should not log again, since the last log was short time ago
    snapshotter._evaluate_memory_load(
        current_memory_usage_bytes=high_memory_usage,
        snapshot_timestamp=datetime.now(tz=timezone.utc),
    )

    assert mock_logger_warn.call_count == 1


def test_evaluate_memory_load_low(monkeypatch: pytest.MonkeyPatch, snapshotter: Snapshotter) -> None:
    mock_logger_warn = MagicMock()
    monkeypatch.setattr(getLogger('crawlee.autoscaling.snapshotter'), 'warning', mock_logger_warn)
    snapshotter._max_memory_bytes = 8 * 1024**3  # 8 GB

    low_memory_usage = int(0.8 * 8 * 1024**3)  # 20% of 8 GB

    snapshotter._evaluate_memory_load(
        current_memory_usage_bytes=low_memory_usage,
        snapshot_timestamp=datetime.now(tz=timezone.utc),
    )

    assert mock_logger_warn.call_count == 0


#
