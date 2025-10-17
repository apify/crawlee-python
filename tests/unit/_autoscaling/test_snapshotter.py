from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

import pytest

from crawlee import service_locator
from crawlee._autoscaling import Snapshotter
from crawlee._autoscaling._types import ClientSnapshot, CpuSnapshot, MemorySnapshot
from crawlee._autoscaling.snapshotter import SortedSnapshotList
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import CpuInfo, MemoryInfo
from crawlee.configuration import Configuration
from crawlee.events import LocalEventManager
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def event_manager() -> AsyncGenerator[LocalEventManager, None]:
    # Use a long interval to avoid interference from periodic system info events during tests
    async with LocalEventManager(system_info_interval=timedelta(hours=9999)) as event_manager:
        yield event_manager


@pytest.fixture
async def snapshotter(event_manager: LocalEventManager) -> AsyncGenerator[Snapshotter, None]:
    config = Configuration(available_memory_ratio=0.25)
    service_locator.set_event_manager(event_manager)
    async with Snapshotter.from_config(config) as snapshotter:
        yield snapshotter


@pytest.fixture
def default_cpu_info() -> CpuInfo:
    return CpuInfo(used_ratio=0.5)


@pytest.fixture
def default_memory_info() -> MemoryInfo:
    return MemoryInfo(
        total_size=ByteSize.from_gb(8),
        current_size=ByteSize.from_gb(4),
        system_wide_used_size=ByteSize.from_gb(5),
    )


@pytest.fixture
def event_system_data_info(default_cpu_info: CpuInfo, default_memory_info: MemoryInfo) -> EventSystemInfoData:
    return EventSystemInfoData(
        cpu_info=default_cpu_info,
        memory_info=default_memory_info,
    )


async def test_start_stop_lifecycle() -> None:
    config = Configuration(available_memory_ratio=0.25)

    async with Snapshotter.from_config(config):
        pass


async def test_snapshot_cpu(
    snapshotter: Snapshotter, event_system_data_info: EventSystemInfoData, event_manager: LocalEventManager
) -> None:
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_data_info)
    await event_manager.wait_for_all_listeners_to_complete()
    cpu_snapshots = cast('list[CpuSnapshot]', snapshotter.get_cpu_sample())
    assert len(cpu_snapshots) == 1
    assert cpu_snapshots[0].used_ratio == event_system_data_info.cpu_info.used_ratio


async def test_snapshot_memory(
    snapshotter: Snapshotter, event_system_data_info: EventSystemInfoData, event_manager: LocalEventManager
) -> None:
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_system_data_info)
    await event_manager.wait_for_all_listeners_to_complete()
    memory_snapshots = cast('list[MemorySnapshot]', snapshotter.get_memory_sample())
    assert len(memory_snapshots) == 1
    assert memory_snapshots[0].current_size == event_system_data_info.memory_info.current_size


async def test_snapshot_memory_with_memory_info_sets_system_wide_fields(
    snapshotter: Snapshotter, event_manager: LocalEventManager
) -> None:
    memory_info = MemoryInfo(
        total_size=ByteSize.from_gb(16),
        current_size=ByteSize.from_gb(4),
        system_wide_used_size=ByteSize.from_gb(12),
    )

    event_data = EventSystemInfoData(
        cpu_info=CpuInfo(used_ratio=0.5),
        memory_info=memory_info,
    )

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
    await event_manager.wait_for_all_listeners_to_complete()

    memory_snapshots = cast('list[MemorySnapshot]', snapshotter.get_memory_sample())

    assert len(memory_snapshots) == 1
    memory_snapshot = memory_snapshots[0]

    # Test that system-wide fields are properly set
    assert memory_snapshot.system_wide_used_size == memory_info.system_wide_used_size
    assert memory_snapshot.system_wide_memory_size == memory_info.total_size


def test_snapshot_event_loop(snapshotter: Snapshotter) -> None:
    # A first event loop snapshot is created when an instance is created.
    event_loop_snapshots = snapshotter.get_event_loop_sample()
    assert len(event_loop_snapshots) == 1


def test_snapshot_client(snapshotter: Snapshotter) -> None:
    # A first client snapshot is created when an instance is created.
    client_snapshots = snapshotter.get_client_sample()
    assert len(client_snapshots) == 1


def test_snapshot_client_overloaded() -> None:
    assert not ClientSnapshot(error_count=1, new_error_count=1, max_error_count=2).is_overloaded
    assert not ClientSnapshot(error_count=2, new_error_count=1, max_error_count=2).is_overloaded
    assert not ClientSnapshot(error_count=4, new_error_count=2, max_error_count=2).is_overloaded
    assert ClientSnapshot(error_count=7, new_error_count=3, max_error_count=2).is_overloaded


async def test_get_cpu_sample(
    snapshotter: Snapshotter, event_manager: LocalEventManager, default_memory_info: MemoryInfo
) -> None:
    now = datetime.now(timezone.utc)
    snapshotter._SNAPSHOT_HISTORY = timedelta(hours=10)  # Extend history for testing

    events_data = [
        EventSystemInfoData(
            cpu_info=CpuInfo(used_ratio=0.5, created_at=now - timedelta(hours=delta)),
            memory_info=default_memory_info,
        )
        for delta in range(5, 0, -1)
    ]
    for event_data in events_data:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
    await event_manager.wait_for_all_listeners_to_complete()

    # When no sample duration is provided it should return all snapshots
    samples = snapshotter.get_cpu_sample()
    assert len(samples) == len(events_data)

    duration = timedelta(hours=0.5)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == 1

    duration = timedelta(hours=2.5)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == 3

    duration = timedelta(hours=10)
    samples = snapshotter.get_cpu_sample(duration)
    assert len(samples) == len(events_data)


async def test_methods_raise_error_when_not_active() -> None:
    snapshotter = Snapshotter.from_config(Configuration(available_memory_ratio=0.25))
    assert snapshotter.active is False

    with pytest.raises(RuntimeError, match=r'Snapshotter is not active.'):
        snapshotter.get_cpu_sample()

    with pytest.raises(RuntimeError, match=r'Snapshotter is not active.'):
        snapshotter.get_memory_sample()

    with pytest.raises(RuntimeError, match=r'Snapshotter is not active.'):
        snapshotter.get_event_loop_sample()

    with pytest.raises(RuntimeError, match=r'Snapshotter is not active.'):
        snapshotter.get_client_sample()

    with pytest.raises(RuntimeError, match=r'Snapshotter is already active.'):
        async with snapshotter, snapshotter:
            pass

    async with snapshotter:
        snapshotter.get_cpu_sample()
        snapshotter.get_memory_sample()
        snapshotter.get_event_loop_sample()
        snapshotter.get_client_sample()

        assert snapshotter.active is True


async def test_snapshot_pruning_removes_outdated_records(
    snapshotter: Snapshotter, event_manager: LocalEventManager, default_memory_info: MemoryInfo
) -> None:
    # Set the snapshot history to 2 hours
    snapshotter._SNAPSHOT_HISTORY = timedelta(hours=2)

    # Create timestamps for testing
    now = datetime.now(timezone.utc)

    events_data = [
        EventSystemInfoData(
            cpu_info=CpuInfo(used_ratio=0.5, created_at=now - timedelta(hours=delta)),
            memory_info=default_memory_info,
        )
        for delta in [5, 3, 2, 0]
    ]

    for event_data in events_data:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
    await event_manager.wait_for_all_listeners_to_complete()

    cpu_snapshots = cast('list[CpuSnapshot]', snapshotter.get_cpu_sample())

    # Check that only the last two snapshots remain
    assert len(cpu_snapshots) == 2
    assert cpu_snapshots[0].created_at == now - timedelta(hours=2)
    assert cpu_snapshots[1].created_at == now


async def test_memory_load_evaluation_logs_warning_on_high_usage(
    caplog: pytest.LogCaptureFixture,
    event_manager: LocalEventManager,
    default_cpu_info: CpuInfo,
) -> None:
    config = Configuration(memory_mbytes=8192)

    service_locator.set_event_manager(event_manager)
    snapshotter = Snapshotter.from_config(config)

    high_memory_usage = ByteSize.from_gb(8) * 0.95  # 95% of 8 GB

    event_data = EventSystemInfoData(
        cpu_info=default_cpu_info,
        memory_info=MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=high_memory_usage,
            system_wide_used_size=ByteSize.from_gb(7),
        ),
    )

    async with snapshotter:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
        await event_manager.wait_for_all_listeners_to_complete()

        # Filter log records to only include those from snapshotter
        log_records = [record for record in caplog.records if 'snapshotter' in record.pathname.lower()]

        assert len(log_records) == 1
        assert log_records[0].levelname.lower() == 'warning'
        assert 'Memory is critically overloaded' in log_records[0].msg

        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
        await event_manager.wait_for_all_listeners_to_complete()

        log_records = [record for record in caplog.records if 'snapshotter' in record.pathname.lower()]

        assert len(log_records) == 1


async def test_memory_load_evaluation_silent_on_acceptable_usage(
    monkeypatch: pytest.MonkeyPatch,
    event_manager: LocalEventManager,
    default_cpu_info: CpuInfo,
) -> None:
    mock_logger_warn = MagicMock()
    monkeypatch.setattr(getLogger('crawlee.autoscaling.snapshotter'), 'warning', mock_logger_warn)

    service_locator.set_event_manager(event_manager)
    snapshotter = Snapshotter.from_config(Configuration(memory_mbytes=8192))

    low_memory_usage = ByteSize.from_gb(8) * 0.8  # 80% of 8 GB

    event_data = EventSystemInfoData(
        cpu_info=default_cpu_info,
        memory_info=MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=low_memory_usage,
            system_wide_used_size=ByteSize.from_gb(7),
        ),
    )

    async with snapshotter:
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)
        await event_manager.wait_for_all_listeners_to_complete()

        assert mock_logger_warn.call_count == 0


async def test_snapshots_time_ordered(snapshotter: Snapshotter, event_manager: LocalEventManager) -> None:
    # All internal snapshot list should be ordered by creation time in ascending order.
    # Scenario where older emitted event arrives after newer event.
    # Snapshotter should not trust the event order and check events' times.
    time_new = datetime.now(tz=timezone.utc)
    time_old = datetime.now(tz=timezone.utc) - timedelta(milliseconds=50)

    def create_event_data(creation_time: datetime) -> EventSystemInfoData:
        return EventSystemInfoData(
            cpu_info=CpuInfo(used_ratio=0.5, created_at=creation_time),
            memory_info=MemoryInfo(
                current_size=ByteSize(bytes=1),
                created_at=creation_time,
                total_size=ByteSize(bytes=2),
                system_wide_used_size=ByteSize.from_gb(5),
            ),
        )

    event_manager.emit(event=Event.SYSTEM_INFO, event_data=create_event_data(time_new))
    event_manager.emit(event=Event.SYSTEM_INFO, event_data=create_event_data(time_old))
    await event_manager.wait_for_all_listeners_to_complete()

    memory_samples = snapshotter.get_memory_sample()
    cpu_samples = snapshotter.get_cpu_sample()
    assert memory_samples[0].created_at == time_old
    assert cpu_samples[0].created_at == time_old
    assert memory_samples[1].created_at == time_new
    assert cpu_samples[1].created_at == time_new


def test_sorted_snapshot_list_add_maintains_order() -> None:
    """Test that SortedSnapshotList.add method maintains sorted order by created_at with multiple items."""
    sorted_list = SortedSnapshotList[CpuSnapshot]()

    # Create snapshots with different timestamps (more items to test binary search better)
    now = datetime.now(timezone.utc)
    snapshots = [
        CpuSnapshot(used_ratio=0.1, max_used_ratio=0.95, created_at=now - timedelta(seconds=50)),  # oldest
        CpuSnapshot(used_ratio=0.2, max_used_ratio=0.95, created_at=now - timedelta(seconds=40)),
        CpuSnapshot(used_ratio=0.3, max_used_ratio=0.95, created_at=now - timedelta(seconds=30)),
        CpuSnapshot(used_ratio=0.4, max_used_ratio=0.95, created_at=now - timedelta(seconds=20)),
        CpuSnapshot(used_ratio=0.5, max_used_ratio=0.95, created_at=now - timedelta(seconds=10)),
        CpuSnapshot(used_ratio=0.6, max_used_ratio=0.95, created_at=now - timedelta(seconds=5)),
        CpuSnapshot(used_ratio=0.7, max_used_ratio=0.95, created_at=now),  # newest
    ]

    # Add snapshots in random order to test binary search insertion
    add_order = [3, 0, 5, 1, 6, 2, 4]  # indices in random order
    for i in add_order:
        sorted_list.add(snapshots[i])

    # Verify the list is sorted by created_at (should be in original order)
    assert len(sorted_list) == 7
    for i, snapshot in enumerate(sorted_list):
        assert snapshot == snapshots[i], f'Item at index {i} is not correctly sorted'
        if i > 0:
            prev_time = sorted_list[i - 1].created_at
            curr_time = snapshot.created_at
            assert prev_time <= curr_time, f'Items at indices {i - 1} and {i} are not in chronological order'
