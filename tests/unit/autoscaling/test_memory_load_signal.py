from __future__ import annotations

from datetime import timedelta
from logging import WARNING, getLogger
from math import floor
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import proclimits
import pytest

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce
from crawlee._utils.system import CpuInfo, MemoryInfo, MemoryUsageInfo, get_memory_info
from crawlee.autoscaling import LoadSignalStartContext, MemoryLoadSignal
from crawlee.autoscaling import _memory_load_signal as memory_load_signal_module
from crawlee.autoscaling._memory_load_signal import SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD
from crawlee.configuration import Configuration
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from crawlee.events import LocalEventManager


def start_context(configuration: Configuration | None = None) -> LoadSignalStartContext:
    """Build the context a signal starts with, sampled over 30 seconds."""
    return LoadSignalStartContext(
        max_sample_window=timedelta(seconds=30), configuration=configuration or Configuration()
    )


async def emit_memory_info(event_manager: LocalEventManager, *memory_infos: MemoryUsageInfo) -> None:
    """Emit one `SYSTEM_INFO` event per memory info and wait until the listeners handle them."""
    for memory_info in memory_infos:
        event_data = EventSystemInfoData(cpu_info=CpuInfo(used_ratio=0.5), memory_info=memory_info)
        event_manager.emit(event=Event.SYSTEM_INFO, event_data=event_data)

    await event_manager.wait_for_all_listeners_to_complete()


async def test_memory_snapshot_from_event(event_manager: LocalEventManager) -> None:
    """A `SYSTEM_INFO` event records the memory used by the process."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(available_memory_ratio=0.25)))

    memory_info = MemoryInfo(
        total_size=ByteSize.from_gb(8),
        current_size=ByteSize.from_gb(4),
        system_wide_used_size=ByteSize.from_gb(5),
    )
    await emit_memory_info(event_manager, memory_info)
    await signal.stop()

    sample = signal.get_sample()
    assert len(sample) == 1
    assert sample[0].current_size == memory_info.current_size


async def test_memory_stop_unsubscribes(event_manager: LocalEventManager) -> None:
    """A stopped signal records no further snapshots."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=8192)))
    await signal.stop()

    await emit_memory_info(
        event_manager,
        MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_gb(1),
            system_wide_used_size=ByteSize.from_gb(2),
        ),
    )

    assert signal.get_sample() == []


async def test_memory_restart_drops_snapshots(event_manager: LocalEventManager) -> None:
    """A restarted signal is not judged on measurements from before the downtime."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=8192)))

    await emit_memory_info(
        event_manager,
        MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_gb(1),
            system_wide_used_size=ByteSize.from_gb(2),
        ),
    )
    await signal.stop()

    await signal.start(start_context(Configuration(memory_mbytes=8192)))
    await signal.stop()

    assert signal.get_sample() == []


async def test_memory_limit_from_configuration(event_manager: LocalEventManager) -> None:
    """Memory counts as overloaded once the process uses more than `Configuration.max_used_memory_ratio`."""
    configuration = Configuration(memory_mbytes=1000, max_used_memory_ratio=0.5)
    signal = MemoryLoadSignal()
    await signal.start(start_context(configuration))

    # The process uses 60% of its budget.
    await emit_memory_info(
        event_manager,
        MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_mb(600),
            system_wide_used_size=ByteSize.from_gb(1),
        ),
    )
    await signal.stop()

    assert signal.get_sample()[0].is_overloaded


async def test_memory_overloaded_system_wide(event_manager: LocalEventManager) -> None:
    """Memory counts as overloaded once the memory used under the same limit goes above 97% of it."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=8192)))

    # The process uses little of its budget, so only the system-wide usage decides.
    await emit_memory_info(
        event_manager,
        MemoryInfo(
            total_size=ByteSize.from_gb(32),
            current_size=ByteSize.from_gb(1),
            system_wide_used_size=ByteSize.from_gb(31),  # 96.9%
        ),
        MemoryInfo(
            total_size=ByteSize.from_gb(32),
            current_size=ByteSize.from_gb(1),
            system_wide_used_size=ByteSize.from_gb(31.5),  # 98.4%
        ),
    )
    await signal.stop()

    assert [snapshot.is_overloaded for snapshot in signal.get_sample()] == [False, True]


async def test_memory_overloaded_without_system_wide_info(event_manager: LocalEventManager) -> None:
    """Without system-wide figures in the event, only the process usage against the budget decides."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=1000, max_used_memory_ratio=0.5)))

    await emit_memory_info(
        event_manager,
        MemoryUsageInfo(current_size=ByteSize.from_mb(400)),
        MemoryUsageInfo(current_size=ByteSize.from_mb(600)),
    )
    await signal.stop()

    assert [snapshot.is_overloaded for snapshot in signal.get_sample()] == [False, True]


async def test_memory_warns_on_critical_usage(
    caplog: pytest.LogCaptureFixture,
    event_manager: LocalEventManager,
) -> None:
    """Critical memory usage is reported once per cooldown period."""
    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=8192)))

    memory_info = MemoryInfo(
        total_size=ByteSize.from_gb(8),
        current_size=ByteSize.from_gb(8) * 0.95,
        system_wide_used_size=ByteSize.from_gb(7),
    )

    with caplog.at_level(WARNING, logger=memory_load_signal_module.logger.name):
        await emit_memory_info(event_manager, memory_info)
        await emit_memory_info(event_manager, memory_info)

    await signal.stop()

    warnings = [record.message for record in caplog.records if 'Memory is critically overloaded' in record.message]
    assert len(warnings) == 1


async def test_memory_silent_on_acceptable_usage(
    monkeypatch: pytest.MonkeyPatch,
    event_manager: LocalEventManager,
) -> None:
    """Memory usage below the critical threshold logs no warning."""
    mock_logger_warn = Mock()
    monkeypatch.setattr(getLogger('crawlee.autoscaling._memory_load_signal'), 'warning', mock_logger_warn)

    signal = MemoryLoadSignal()
    await signal.start(start_context(Configuration(memory_mbytes=8192)))

    await emit_memory_info(
        event_manager,
        MemoryInfo(
            total_size=ByteSize.from_gb(8),
            current_size=ByteSize.from_gb(8) * 0.8,
            system_wide_used_size=ByteSize.from_gb(7),
        ),
    )
    await signal.stop()

    assert mock_logger_warn.call_count == 0


@pytest.mark.parametrize(
    'dynamic_memory',
    [
        pytest.param(True, id='ratio of available memory'),
        pytest.param(False, id='fixed memory_mbytes'),
    ],
)
async def test_dynamic_memory(*, event_manager: LocalEventManager, dynamic_memory: bool) -> None:
    """A budget given as a ratio follows the memory available to the process, a fixed one does not."""
    initial_memory_info = get_memory_info()
    ratio_just_below_system_wide_overload = 0.99 * SYSTEM_WIDE_MEMORY_OVERLOAD_THRESHOLD
    memory_mbytes = 0 if dynamic_memory else floor(initial_memory_info.total_size.to_mb())
    used_size = initial_memory_info.total_size * ratio_just_below_system_wide_overload

    configuration = Configuration(
        memory_mbytes=memory_mbytes, available_memory_ratio=ratio_just_below_system_wide_overload
    )
    signal = MemoryLoadSignal()
    await signal.start(start_context(configuration))

    await emit_memory_info(
        event_manager,
        # Memory usage exactly at the overload threshold, but not overloaded system-wide.
        MemoryInfo(
            total_size=initial_memory_info.total_size,
            current_size=used_size,
            system_wide_used_size=used_size,
        ),
        # The same usage, with twice as much memory available.
        MemoryInfo(
            total_size=initial_memory_info.total_size * 2,
            current_size=used_size,
            system_wide_used_size=used_size,
        ),
    )
    await signal.stop()

    sample = signal.get_sample()
    assert len(sample) == 2
    assert sample[0].is_overloaded
    assert sample[1].is_overloaded == (not dynamic_memory)


@pytest.mark.parametrize(
    ('config_options', 'limit_bytes', 'expected_warning'),
    [
        pytest.param({}, 512 * 1024**2, True, id='default ratio under a limit'),
        pytest.param({'available_memory_ratio': 0.25}, 512 * 1024**2, False, id='ratio set explicitly'),
        pytest.param({'memory_mbytes': 1024}, 512 * 1024**2, False, id='memory_mbytes set'),
        pytest.param({}, None, False, id='no limit'),
    ],
)
async def test_warns_on_default_ratio_under_limit(
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    config_options: dict[str, Any],
    limit_bytes: int | None,
    expected_warning: bool,
) -> None:
    """Warns when the default `available_memory_ratio` stacks on top of a memory limit."""
    budget = None if limit_bytes is None else proclimits.MemoryBudget(limit=limit_bytes, used=0, available=limit_bytes)
    monkeypatch.setattr(proclimits, 'get_memory_budget', Mock(return_value=budget))
    monkeypatch.setattr(memory_load_signal_module, 'logger_once', LoggerOnce(memory_load_signal_module.logger))

    signal = MemoryLoadSignal()

    with caplog.at_level(WARNING, logger=memory_load_signal_module.logger.name):
        await signal.start(start_context(Configuration(**config_options)))

    await signal.stop()

    warnings = [record.message for record in caplog.records if 'Setting max memory of this run' in record.message]
    assert len(warnings) == int(expected_warning)
    assert all('to 128.00 MB, 25% of the 512.00 MB memory limit' in warning for warning in warnings)
