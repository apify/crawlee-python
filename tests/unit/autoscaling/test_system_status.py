from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from crawlee.autoscaling import LoadRatioInfo, LoadSnapshot, SnapshotStore, SystemInfo
from crawlee.autoscaling._load_signal import LoadSignal
from crawlee.autoscaling._snapshotter import Snapshotter
from crawlee.autoscaling._system_status import SystemStatus


def load_signal(name: str, snapshots: list[tuple[timedelta, bool]], *, overloaded_ratio: float = 0.5) -> Mock:
    """Build a signal sampling the given `(age, is_overloaded)` snapshots through a real `SnapshotStore`."""
    now = datetime.now(UTC)
    store = SnapshotStore[LoadSnapshot]()

    for age, is_overloaded in snapshots:
        store.push(LoadSnapshot(is_overloaded=is_overloaded, created_at=now - age))

    signal = Mock(spec=LoadSignal)
    signal.name = name
    signal.overloaded_ratio = overloaded_ratio
    signal.get_sample.side_effect = store.get_sample
    return signal


def snapshotter(*load_signals: LoadSignal) -> Mock:
    """Build a snapshotter owning the given signals."""
    return Mock(spec=Snapshotter, load_signals=list(load_signals))


def test_cpu_is_overloaded() -> None:
    """A signal is overloaded once its weighted share of overloaded time exceeds its ratio."""
    cpu_signal = load_signal(
        'cpu_info',
        [
            (timedelta(minutes=3), False),
            (timedelta(minutes=2), False),
            (timedelta(minutes=1), True),
            (timedelta(0), True),
        ],
    )
    system_status = SystemStatus(snapshotter(cpu_signal), snapshot_history=timedelta(minutes=10))

    cpu_info = system_status.get_historical_system_info().cpu_info

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.667)
    assert cpu_info.is_overloaded is True


def test_cpu_is_not_overloaded() -> None:
    """A signal is not overloaded while its weighted share of overloaded time stays below its ratio."""
    cpu_signal = load_signal(
        'cpu_info',
        [
            (timedelta(minutes=3), True),
            (timedelta(minutes=2), True),
            (timedelta(minutes=1), False),
            (timedelta(0), False),
        ],
    )
    system_status = SystemStatus(snapshotter(cpu_signal), snapshot_history=timedelta(minutes=10))

    cpu_info = system_status.get_historical_system_info().cpu_info

    assert cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.333)
    assert cpu_info.is_overloaded is False


def test_get_system_info() -> None:
    """The current status looks back `current_history`, the historical one `snapshot_history`."""
    minutes = [timedelta(minutes=age) for age in (3, 2, 1, 0)]
    seconds = [timedelta(seconds=age) for age in (90, 60, 30, 0)]

    signals = [
        load_signal('cpu_info', list(zip(minutes, [False, False, True, True], strict=True))),
        load_signal('memory_info', list(zip(seconds, [False, True, True, False], strict=True))),
        load_signal('event_loop_info', list(zip(minutes, [True, True, False, False], strict=True))),
        load_signal('storage_client_info', list(zip(minutes, [False, False, False, False], strict=True))),
    ]
    system_status = SystemStatus(
        snapshotter(*signals),
        current_history=timedelta(minutes=1),
        snapshot_history=timedelta(minutes=10),
    )

    current_system_info = system_status.get_current_system_info()
    assert current_system_info == SystemInfo(
        cpu_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=1.0),
        memory_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.5),
        event_loop_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0),
        storage_client_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0),
        created_at=current_system_info.created_at,
    )
    assert current_system_info.is_system_idle is False

    historical_system_info = system_status.get_historical_system_info()
    assert historical_system_info == SystemInfo(
        cpu_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.667),
        memory_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.667),
        event_loop_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0.333),
        storage_client_info=LoadRatioInfo(limit_ratio=0.5, actual_ratio=0),
        created_at=historical_system_info.created_at,
    )
    assert historical_system_info.is_system_idle is False


@pytest.mark.parametrize(
    ('snapshot_history', 'is_system_idle'),
    [
        pytest.param(timedelta(seconds=30), True, id='old overload outside the window'),
        pytest.param(timedelta(minutes=5), False, id='old overload within the window'),
    ],
)
def test_historical_window(*, snapshot_history: timedelta, is_system_idle: bool) -> None:
    """The historical status looks back only `snapshot_history`, however much history a signal keeps."""
    signal = load_signal(
        'proxy_health',
        [
            (timedelta(minutes=2), True),
            (timedelta(minutes=1, seconds=59), True),
            (timedelta(seconds=1), False),
            (timedelta(0), False),
        ],
        overloaded_ratio=0,
    )
    system_status = SystemStatus(snapshotter(signal), snapshot_history=snapshot_history)

    assert system_status.get_historical_system_info().is_system_idle is is_system_idle


@pytest.mark.parametrize(
    ('overloaded_ratio', 'is_overloaded'),
    [
        pytest.param(0.66, True, id='below the share'),
        pytest.param(0.67, False, id='above the share'),
    ],
)
def test_overloaded_ratio(*, overloaded_ratio: float, is_overloaded: bool) -> None:
    """The signal's own ratio decides whether its share of overloaded time counts as overloaded."""
    signal = load_signal(
        'storage_client_info',
        [
            (timedelta(minutes=3), True),
            (timedelta(minutes=2), True),
            (timedelta(minutes=1), True),
            (timedelta(0), False),
        ],
        overloaded_ratio=overloaded_ratio,
    )
    system_status = SystemStatus(snapshotter(signal), snapshot_history=timedelta(minutes=10))

    # Ratio of overloaded snapshots is 2/3 (2 minutes out of 3)
    assert system_status.get_historical_system_info().storage_client_info.is_overloaded == is_overloaded


def test_simultaneous_snapshots_count() -> None:
    """Snapshots taken at the same time still carry weight in the average."""
    signal = load_signal('cpu_info', [(timedelta(0), False), (timedelta(0), True)])
    system_status = SystemStatus(snapshotter(signal))

    assert system_status.get_historical_system_info().cpu_info.actual_ratio == 1.0


def test_custom_signal_reported_by_name() -> None:
    """A custom signal is reported under its name, a built-in one switched off as not overloaded."""
    custom_signal = load_signal('proxy_health', [(timedelta(seconds=1), True), (timedelta(0), True)])
    system_status = SystemStatus(snapshotter(custom_signal))

    system_info = system_status.get_current_system_info()

    assert system_info.load_signal_info == {'proxy_health': LoadRatioInfo(limit_ratio=0.5, actual_ratio=1.0)}
    assert system_info.cpu_info == LoadRatioInfo(limit_ratio=0, actual_ratio=0)
    assert system_info.is_system_idle is False
    assert str(system_info) == 'overloaded: proxy_health 100% of the time (limit 50%)'


def test_custom_signal_named_like_resource_keeps_builtin_in_log() -> None:
    """A custom signal named like a built-in resource does not hide that resource's overload in the log line."""
    cpu_signal = load_signal('cpu_info', [(timedelta(seconds=1), True), (timedelta(0), True)])
    custom_signal = load_signal('cpu', [(timedelta(seconds=1), False), (timedelta(0), False)])
    system_status = SystemStatus(snapshotter(cpu_signal, custom_signal))

    assert str(system_status.get_current_system_info()) == 'overloaded: cpu 100% of the time (limit 50%)'


def test_custom_signal_takes_builtin_name() -> None:
    """A custom signal named after a built-in one that was switched off is reported in its field."""
    custom_signal = load_signal('cpu_info', [(timedelta(seconds=1), True), (timedelta(0), True)])
    system_status = SystemStatus(snapshotter(custom_signal))

    system_info = system_status.get_current_system_info()

    assert system_info.cpu_info == LoadRatioInfo(limit_ratio=0.5, actual_ratio=1.0)
    assert system_info.load_signal_info == {}
