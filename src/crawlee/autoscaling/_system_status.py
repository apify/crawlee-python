from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from more_itertools import pairwise

from crawlee._utils.docs import docs_group

from ._cpu_load_signal import CpuLoadSignal
from ._event_loop_load_signal import EventLoopLoadSignal
from ._memory_load_signal import MemoryLoadSignal
from ._storage_client_load_signal import StorageClientLoadSignal
from ._types import LoadRatioInfo, SystemInfo

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ._load_signal import LoadSnapshot
    from ._snapshotter import Snapshotter

logger = getLogger(__name__)

_BUILTIN_SIGNAL_OPTION_KEYS = {
    MemoryLoadSignal.name: 'memory',
    EventLoopLoadSignal.name: 'event_loop',
    CpuLoadSignal.name: 'cpu',
    StorageClientLoadSignal.name: 'storage_client',
}
"""The names of the built-in signals, which map to `SystemInfo` fields, and the options that switch each off."""


@docs_group('Autoscaling')
class SystemStatus:
    """Evaluates the load signals of a `ConcurrencySystem` and combines them into a `SystemInfo`.

    Each signal is evaluated as a weighted average of its overloaded snapshots, with the weights being the time
    intervals between the snapshots. The system is considered as overloaded whenever at least one signal is overloaded.

    Every signal is evaluated over two windows. `get_current_system_info` looks back `current_history` and gates the
    start of each new task. `get_historical_system_info` looks back `snapshot_history` and drives the autoscaling.
    """

    def __init__(
        self,
        snapshotter: Snapshotter,
        *,
        current_history: timedelta = timedelta(seconds=5),
        snapshot_history: timedelta = timedelta(seconds=30),
    ) -> None:
        """Initialize a new instance.

        Args:
            snapshotter: The `Snapshotter` whose signals are evaluated.
            current_history: How far back `get_current_system_info` looks.
            snapshot_history: How far back `get_historical_system_info` looks.

        Raises:
            ValueError: If two signals share a name.
        """
        self._load_signals = snapshotter.load_signals
        self._current_history = current_history
        self._snapshot_history = snapshot_history
        self._raise_on_duplicate_names()

    def get_current_system_info(self) -> SystemInfo:
        """Retrieve and evaluate the current status of system resources, over the last `current_history`.

        Returns:
            An object representing the current system status.
        """
        return self._get_system_info(self._current_history)

    def get_historical_system_info(self) -> SystemInfo:
        """Retrieve and evaluate the historical status of system resources, over the last `snapshot_history`.

        Returns:
            An object representing the historical system status.
        """
        return self._get_system_info(self._snapshot_history)

    def _raise_on_duplicate_names(self) -> None:
        """Refuse signals sharing a name, as only the last of them would show in the reported `SystemInfo`."""
        seen_names = set[str]()

        for signal in self._load_signals:
            if signal.name not in seen_names:
                seen_names.add(signal.name)
                continue

            option_key = _BUILTIN_SIGNAL_OPTION_KEYS.get(signal.name)

            if option_key is None:
                hint = 'two custom signals cannot share a name, so rename one of them.'
            else:
                hint = (
                    f'it is the name of a built-in signal. To replace that signal, switch it off with '
                    f'`LoadSignalsOptions({option_key}=False)` and keep your implementation in `custom`. To run yours '
                    f'alongside it, give it a different name.'
                )

            raise ValueError(f'Duplicate load signal name {signal.name!r}: {hint}')

    def _get_system_info(self, sample_duration: timedelta) -> SystemInfo:
        """Get system information based on the overload state of the signals within `sample_duration`.

        A built-in signal that was switched off is reported as not overloaded.
        """
        load_signal_info = {
            signal.name: self._is_sample_overloaded(signal.get_sample(sample_duration), signal.overloaded_ratio)
            for signal in self._load_signals
        }

        unwatched = LoadRatioInfo(limit_ratio=0, actual_ratio=0)

        return SystemInfo(
            memory_info=load_signal_info.pop(MemoryLoadSignal.name, unwatched),
            event_loop_info=load_signal_info.pop(EventLoopLoadSignal.name, unwatched),
            cpu_info=load_signal_info.pop(CpuLoadSignal.name, unwatched),
            storage_client_info=load_signal_info.pop(StorageClientLoadSignal.name, unwatched),
            load_signal_info=load_signal_info,
        )

    def _is_sample_overloaded(self, sample: Sequence[LoadSnapshot], threshold: float) -> LoadRatioInfo:
        """Determine if a sample of snapshot data is overloaded based on a specified ratio.

        Args:
            sample: A list of snapshot data to analyze.
            threshold: The threshold ratio to use for determining if the sample is overloaded.

        Returns:
            An object with an `is_overloaded` property set to `True` if the sample is considered overloaded based
            on the specified threshold ratio. Otherwise, `is_overloaded` is set to `False`.
        """
        if not sample:
            return LoadRatioInfo(limit_ratio=threshold, actual_ratio=0)

        if len(sample) == 1:
            return LoadRatioInfo(limit_ratio=threshold, actual_ratio=float(sample[0].is_overloaded))

        overloaded_time = timedelta(0)
        total_time = timedelta(0)

        for previous, current in pairwise(sample):
            time = current.created_at - previous.created_at
            if time < timedelta(0):
                raise ValueError('Negative time. Code assumptions are not valid. Expected time sorted samples.')

            # Snapshots taken at the same time still carry weight, so they are not dropped from the average.
            time = time or timedelta(milliseconds=1)

            total_time += time
            if current.is_overloaded:
                overloaded_time += time

        return LoadRatioInfo(limit_ratio=threshold, actual_ratio=round(overloaded_time / total_time, 3))
