from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee._autoscaling import Snapshotter
from crawlee._autoscaling.types import CpuSnapshot, EventLoopSnapshot, Snapshot
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import CpuInfo, MemoryInfo
from crawlee.events import EventManager, LocalEventManager
from crawlee.events._types import EventSystemInfoData


@pytest.fixture
def snapshotter() -> Snapshotter:
    mocked_event_manager = AsyncMock(spec=EventManager)
    return Snapshotter(mocked_event_manager, available_memory_ratio=0.25)


@pytest.fixture
def event_system_data_info() -> EventSystemInfoData:
    return EventSystemInfoData(
        cpu_info=CpuInfo(used_ratio=0.5),
        memory_info=MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_gb(4),
        ),
    )


async def test_start_stop_lifecycle() -> None:
    async with LocalEventManager() as event_manager, Snapshotter(event_manager, available_memory_ratio=0.25):
        pass


def test_snapshot_cpu(snapshotter: Snapshotter, event_system_data_info: EventSystemInfoData) -> None:
    snapshotter._snapshot_cpu(event_system_data_info)
    assert len(snapshotter._cpu_snapshots) == 1
    assert snapshotter._cpu_snapshots[0].used_ratio == event_system_data_info.cpu_info.used_ratio


def test_snapshot_memory(snapshotter: Snapshotter, event_system_data_info: EventSystemInfoData) -> None:
    snapshotter._snapshot_memory(event_system_data_info)
    assert len(snapshotter._memory_snapshots) == 1
    assert snapshotter._memory_snapshots[0].current_size == event_system_data_info.memory_info.current_size
    assert snapshotter._memory_snapshots[0].total_size == event_system_data_info.memory_info.total_size


def test_snapshot_event_loop(snapshotter: Snapshotter) -> None:
    snapshotter._event_loop_snapshots = [
        EventLoopSnapshot(delay=timedelta(milliseconds=100), max_delay=timedelta(milliseconds=500)),
    ]

    snapshotter._snapshot_event_loop()
    assert len(snapshotter._event_loop_snapshots) == 2


def test_snapshot_client(snapshotter: Snapshotter) -> None:
    snapshotter._snapshot_client()
    assert len(snapshotter._client_snapshots) == 1


def test_get_cpu_sample(snapshotter: Snapshotter) -> None:
    now = datetime.now(timezone.utc)
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


def test_empty_snapshot_samples_return_empty_lists(snapshotter: Snapshotter) -> None:
    # All get resource sample uses the same helper function, so testing only one of them properly (CPU) should be
    # enough. Here just call all of them returning empty list to make sure they don't crash.
    assert snapshotter.get_cpu_sample() == []
    assert snapshotter.get_memory_sample() == []
    assert snapshotter.get_event_loop_sample() == []
    assert snapshotter.get_client_sample() == []
    assert snapshotter._get_sample([], timedelta(hours=1)) == []


def test_snapshot_pruning_removes_outdated_records(snapshotter: Snapshotter) -> None:
    # Set the snapshot history to 2 hours
    snapshotter._snapshot_history = timedelta(hours=2)

    # Create timestamps for testing
    now = datetime.now(timezone.utc)
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


def test_pruning_empty_snapshot_list_remains_empty(snapshotter: Snapshotter) -> None:
    now = datetime.now(timezone.utc)
    snapshotter._cpu_snapshots = []
    snapshots_casted = cast(list[Snapshot], snapshotter._cpu_snapshots)
    snapshotter._prune_snapshots(snapshots_casted, now)
    assert snapshotter._cpu_snapshots == []


def test_snapshot_pruning_keeps_recent_records_unaffected(snapshotter: Snapshotter) -> None:
    snapshotter._snapshot_history = timedelta(hours=2)

    # Create timestamps for testing
    now = datetime.now(timezone.utc)
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


def test_memory_load_evaluation_logs_warning_on_high_usage(caplog: pytest.LogCaptureFixture) -> None:
    snapshotter = Snapshotter(AsyncMock(spec=EventManager), max_memory_size=ByteSize.from_gb(8))

    high_memory_usage = ByteSize.from_gb(8) * 0.95  # 90% of 8 GB

    snapshotter._evaluate_memory_load(
        current_memory_usage_size=high_memory_usage,
        snapshot_timestamp=datetime.now(timezone.utc),
    )

    assert len(caplog.records) == 1
    assert caplog.records[0].levelname.lower() == 'warning'
    assert 'Memory is critically overloaded' in caplog.records[0].msg

    # It should not log again, since the last log was short time ago
    snapshotter._evaluate_memory_load(
        current_memory_usage_size=high_memory_usage,
        snapshot_timestamp=datetime.now(timezone.utc),
    )

    assert len(caplog.records) == 1


def test_memory_load_evaluation_silent_on_acceptable_usage(
    monkeypatch: pytest.MonkeyPatch,
    snapshotter: Snapshotter,
) -> None:
    mock_logger_warn = MagicMock()
    monkeypatch.setattr(getLogger('crawlee.autoscaling.snapshotter'), 'warning', mock_logger_warn)
    snapshotter._max_memory_size = ByteSize.from_gb(8)

    low_memory_usage = ByteSize.from_gb(8) * 0.8  # 80% of 8 GB

    snapshotter._evaluate_memory_load(
        current_memory_usage_size=low_memory_usage,
        snapshot_timestamp=datetime.now(timezone.utc),
    )

    assert mock_logger_warn.call_count == 0
