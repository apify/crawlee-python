from __future__ import annotations

from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

from crawlee import service_locator
from crawlee.autoscaling import (
    CpuLoadSignal,
    EventLoopLoadSignal,
    LoadSignalsOptions,
    LoadSignalStartContext,
    MemoryLoadSignal,
    MemoryLoadSignalOptions,
    StorageClientLoadSignal,
)
from crawlee.autoscaling._load_signal import LoadSignal
from crawlee.autoscaling._snapshotter import Snapshotter
from crawlee.configuration import Configuration

WINDOW = timedelta(seconds=30)


def test_builds_builtin_signals() -> None:
    """Every built-in signal is enabled by default."""
    snapshotter = Snapshotter(LoadSignalsOptions(), max_sample_window=WINDOW)

    signal_types = [type(signal) for signal in snapshotter.load_signals]
    assert signal_types == [MemoryLoadSignal, EventLoopLoadSignal, CpuLoadSignal, StorageClientLoadSignal]


def test_false_switches_signal_off() -> None:
    """A built-in signal switched off with `False` is not built at all."""
    snapshotter = Snapshotter(LoadSignalsOptions(cpu=False, storage_client=False), max_sample_window=WINDOW)

    signal_types = [type(signal) for signal in snapshotter.load_signals]
    assert signal_types == [MemoryLoadSignal, EventLoopLoadSignal]


def test_custom_signals_follow_builtin() -> None:
    """Custom signals are driven together with the built-in ones, after them."""
    custom_signal = Mock(spec=LoadSignal)
    load_signals = LoadSignalsOptions(event_loop=False, cpu=False, storage_client=False, custom=[custom_signal])
    snapshotter = Snapshotter(load_signals, max_sample_window=WINDOW)

    assert snapshotter.load_signals[1:] == [custom_signal]


def test_options_reach_signal() -> None:
    """The options of a built-in signal are passed to its constructor."""
    memory_options = MemoryLoadSignalOptions(overloaded_ratio=0.1)
    snapshotter = Snapshotter(LoadSignalsOptions(memory=memory_options), max_sample_window=WINDOW)

    memory_signal = snapshotter.load_signals[0]
    assert isinstance(memory_signal, MemoryLoadSignal)
    assert memory_signal.overloaded_ratio == 0.1


async def test_signals_start_with_context() -> None:
    """Every signal starts with the window and configuration the snapshotter was given, and stops on exit."""
    custom_signal = Mock(spec=LoadSignal)
    load_signals = LoadSignalsOptions(
        memory=False,
        event_loop=False,
        cpu=False,
        storage_client=False,
        custom=[custom_signal],
    )
    configuration = Configuration()

    async with Snapshotter(load_signals, max_sample_window=WINDOW, configuration=configuration):
        context = LoadSignalStartContext(max_sample_window=WINDOW, configuration=configuration)
        custom_signal.start.assert_awaited_once_with(context)
        custom_signal.stop.assert_not_awaited()

    custom_signal.stop.assert_awaited_once()


async def test_signals_start_with_global_configuration() -> None:
    """Without a configuration of its own, the snapshotter hands the signals the global one."""
    custom_signal = Mock(spec=LoadSignal)
    load_signals = LoadSignalsOptions(
        memory=False,
        event_loop=False,
        cpu=False,
        storage_client=False,
        custom=[custom_signal],
    )

    async with Snapshotter(load_signals, max_sample_window=WINDOW):
        context = custom_signal.start.await_args.args[0]
        assert context.configuration is service_locator.get_configuration()


async def test_failed_start_stops_started_signals() -> None:
    """A signal that fails to start is stopped along with the ones started before it."""
    started_signal = Mock(spec=LoadSignal)
    failing_signal = Mock(spec=LoadSignal)
    failing_signal.start.side_effect = RuntimeError('Signal failure')

    with (
        patch('crawlee.autoscaling._snapshotter.MemoryLoadSignal', return_value=started_signal),
        patch('crawlee.autoscaling._snapshotter.CpuLoadSignal', return_value=failing_signal),
    ):
        snapshotter = Snapshotter(LoadSignalsOptions(event_loop=False, storage_client=False), max_sample_window=WINDOW)

    with pytest.raises(RuntimeError, match='Signal failure'):
        async with snapshotter:
            pass

    started_signal.stop.assert_awaited_once()
    failing_signal.stop.assert_awaited_once()


async def test_failed_stop_stops_other_signals() -> None:
    """A signal that fails to stop leaves none of the others running."""
    memory_signal = Mock(spec=LoadSignal)
    failing_signal = Mock(spec=LoadSignal)
    failing_signal.stop.side_effect = RuntimeError('Signal failure')
    storage_client_signal = Mock(spec=LoadSignal)

    with (
        patch('crawlee.autoscaling._snapshotter.MemoryLoadSignal', return_value=memory_signal),
        patch('crawlee.autoscaling._snapshotter.CpuLoadSignal', return_value=failing_signal),
        patch('crawlee.autoscaling._snapshotter.StorageClientLoadSignal', return_value=storage_client_signal),
    ):
        snapshotter = Snapshotter(LoadSignalsOptions(event_loop=False), max_sample_window=WINDOW)

    with pytest.raises(RuntimeError, match='Signal failure'):
        await snapshotter.__aexit__(None, None, None)

    memory_signal.stop.assert_awaited_once()
    storage_client_signal.stop.assert_awaited_once()
