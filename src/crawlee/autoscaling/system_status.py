# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/system_status.ts

from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from more_itertools import pairwise

from crawlee._utils.math import get_weighted_avg
from crawlee.autoscaling.types import LoadRatioInfo, Snapshot, SystemInfo

if TYPE_CHECKING:
    from crawlee.autoscaling.snapshotter import Snapshotter

logger = getLogger(__name__)


class SystemStatus:
    """Provides a simple interface to reading system status from a `Snapshotter` instance.

    It only exposes two functions `SystemStatus.get_current_status` and `SystemStatus.get_historical_status`.
    The system status is calculated using a weighted average of overloaded messages in the snapshots, with the weights
    being the time intervals between the snapshots. Each resource is calculated separately, and the system
    is overloaded whenever at least one resource is overloaded. The class is used by the `AutoscaledPool` class.

    `SystemStatus.get_current_status` returns a boolean that represents the current status of the system. The length
    of the current timeframe in seconds is configurable by the `currentHistorySecs` option and represents the max age
    of snapshots to be considered for the calculation.

    `SystemStatus.get_historical_status` returns a boolean that represents the long-term status of the system. It
    considers the full snapshot history available in the `Snapshotter` instance.
    """

    def __init__(
        self,
        snapshotter: Snapshotter,
        *,
        current_history: timedelta = timedelta(seconds=5),
        max_memory_overloaded_ratio: float = 0.2,
        max_event_loop_overloaded_ratio: float = 0.6,
        max_cpu_overloaded_ratio: float = 0.4,
        max_client_overloaded_ratio: float = 0.3,
    ) -> None:
        """Creates a new instance.

        Args:
            snapshotter: The `Snapshotter` instance to be queried for `SystemStatus`.

            current_history: Defines max age of snapshots used in the `SystemStatus.get_current_status` measurement.

            max_memory_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a memory sample.
                If the sample exceeds this ratio, the system will be overloaded.

            max_event_loop_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in an event loop sample.
                If the sample exceeds this ratio, the system will be overloaded.

            max_cpu_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a CPU sample. If the sample
                exceeds this ratio, the system will be overloaded.

            max_client_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a Client sample.
                If the sample exceeds this ratio, the system will be overloaded.
        """
        self._snapshotter = snapshotter
        self._current_history = current_history
        self._max_memory_overloaded_ratio = max_memory_overloaded_ratio
        self._max_event_loop_overloaded_ratio = max_event_loop_overloaded_ratio
        self._max_cpu_overloaded_ratio = max_cpu_overloaded_ratio
        self._max_client_overloaded_ratio = max_client_overloaded_ratio

    def get_current_status(self) -> SystemInfo:
        """Get the current system status.

        Returns a `SystemInfo` object where the `is_system_idle` property is `False` if the system has been overloaded
        in the last `current_history` seconds, and `True` otherwise.

        Returns:
            An object representing the current system status.
        """
        return self._get_system_info(self._current_history)

    def get_historical_status(self) -> SystemInfo:
        """Get the historical system status.

        Returns a `SystemInfo` where the `is_system_idle` property is set to `False` if the system has been overloaded
        in the full history of the `Snapshotter` (which is configurable in the `Snapshotter`, and `True` otherwise.

        Returns:
            An object representing the historical system status.
        """
        return self._get_system_info()

    def _get_system_info(self: SystemStatus, sample_duration: timedelta | None = None) -> SystemInfo:
        """Determine if the system is currently idle or overloaded.

        Args:
            sample_duration: The duration within which to analyze system status.

        Returns:
            An object representing the system status with an `is_system_idle` property set to `True` if the system
            has not been overloaded within the specified time duration, and `False` otherwise.
        """
        mem_info = self._is_memory_overloaded(sample_duration)
        event_loop_info = self._is_event_loop_overloaded(sample_duration)
        cpu_info = self._is_cpu_overloaded(sample_duration)
        client_info = self._is_client_overloaded(sample_duration)

        return SystemInfo(
            mem_info=mem_info,
            event_loop_info=event_loop_info,
            cpu_info=cpu_info,
            client_info=client_info,
        )

    def _is_cpu_overloaded(self: SystemStatus, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the CPU has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze CPU snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the CPU has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self._snapshotter.get_cpu_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._max_cpu_overloaded_ratio)

    def _is_memory_overloaded(self: SystemStatus, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if memory has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze memory snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if memory has been overloaded within the specified
            time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self._snapshotter.get_memory_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._max_memory_overloaded_ratio)

    def _is_event_loop_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the event loop has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze event loop snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the event loop has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self._snapshotter.get_event_loop_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._max_event_loop_overloaded_ratio)

    def _is_client_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the client has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze client snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the client has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self._snapshotter.get_client_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._max_client_overloaded_ratio)

    def _is_sample_overloaded(self: SystemStatus, sample: list[Snapshot], max_ratio: float) -> LoadRatioInfo:
        """Determine if a sample of snapshot data is overloaded based on a specified ratio.

        Args:
            sample: A sequence of snapshot data to analyze.
            max_ratio: The ratio threshold to consider the sample as overloaded.

        Returns:
            An object with an `is_overloaded` property set to `True` if the sample is considered overloaded based
            on the specified ratio. Otherwise, `is_overloaded` is set to `False`.
        """
        logger.debug('sample: %s; max_ratio: %s', sample, max_ratio)
        print(f'sample: {sample}; max_ratio: {max_ratio}')

        if not sample:
            return LoadRatioInfo(limit_ratio=max_ratio, actual_ratio=0)

        weights, values = [], []

        for previous, current in pairwise(sample):
            weight = (current.created_at - previous.created_at).total_seconds() or 0.001  # Avoid zero
            value = float(current.is_overloaded)
            weights.append(weight)
            values.append(value)

        print(f'weights: {weights}; values: {values}')

        try:
            weighted_avg = get_weighted_avg(values, weights)
        except ValueError:
            logger.warning('Total weight cannot be zero')
            return LoadRatioInfo(limit_ratio=max_ratio, actual_ratio=0)

        return LoadRatioInfo(limit_ratio=max_ratio, actual_ratio=round(weighted_avg, 3))
