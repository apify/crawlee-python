from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import Mock

from crawlee.autoscaling import EventLoopLoadSignal, EventLoopLoadSignalOptions, LoadSignalStartContext
from crawlee.autoscaling import _event_loop_load_signal as event_loop_load_signal_module
from crawlee.configuration import Configuration

if TYPE_CHECKING:
    import pytest


def start_context(configuration: Configuration | None = None) -> LoadSignalStartContext:
    """Build the context a signal starts with, sampled over 30 seconds."""
    return LoadSignalStartContext(
        max_sample_window=timedelta(seconds=30), configuration=configuration or Configuration()
    )


async def test_event_loop_first_snapshot() -> None:
    """The signal measures right after it starts, and the first snapshot has nothing to be delayed against."""
    signal = EventLoopLoadSignal(EventLoopLoadSignalOptions(snapshot_interval=timedelta(hours=1)))
    await signal.start(start_context())
    await asyncio.sleep(0)
    await signal.stop()

    sample = signal.get_sample()
    assert len(sample) == 1
    assert not sample[0].is_overloaded


async def test_event_loop_overloaded_when_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    """A snapshot taken later than scheduled by more than `Configuration.max_event_loop_delay` marks an overload."""
    start = datetime.now(UTC)
    clock = Mock(
        side_effect=[
            start,
            start + timedelta(milliseconds=1020),  # 20 ms late
            start + timedelta(milliseconds=2060),  # 40 ms late
        ]
    )
    monkeypatch.setattr(event_loop_load_signal_module, 'datetime', Mock(now=clock))

    # Neither the interval nor the limit is the default, so the signal must read both.
    signal = EventLoopLoadSignal(EventLoopLoadSignalOptions(snapshot_interval=timedelta(seconds=1)))
    await signal.start(start_context(Configuration(max_event_loop_delay=timedelta(milliseconds=30))))

    for _ in range(3):
        signal._snapshot()

    # Nothing yields before the stop, so the measuring task is cancelled before it takes a snapshot of its own.
    await signal.stop()

    sample = signal.get_sample()
    assert [snapshot.is_overloaded for snapshot in sample] == [False, False, True]
    assert sample[-1].exceeded == timedelta(milliseconds=10)


async def test_event_loop_restart_ignores_downtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """A restarted signal does not charge the downtime to the event loop."""
    start = datetime.now(UTC)
    clock = Mock(side_effect=[start, start + timedelta(minutes=1)])
    monkeypatch.setattr(event_loop_load_signal_module, 'datetime', Mock(now=clock))

    signal = EventLoopLoadSignal()
    await signal.start(start_context())
    signal._snapshot()
    await signal.stop()

    await signal.start(start_context())
    signal._snapshot()
    await signal.stop()

    assert [snapshot.is_overloaded for snapshot in signal.get_sample()] == [False]
