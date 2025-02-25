# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/system_status.ts

from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING

from more_itertools import pairwise

from crawlee._autoscaling._types import LoadRatioInfo, Snapshot, SystemInfo
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from crawlee._autoscaling import Snapshotter

logger = getLogger(__name__)


@docs_group('Classes')
class SystemStatus:
    """Provides a simple interface for evaluating system resource usage from snapshots collected by `Snapshotter`.

    This class aggregates and interprets snapshots from a Snapshotter instance to evaluate the current and historical
    status of system resources like CPU, memory, event loop, and client API usage. It exposes two methods
    `get_current_system_info` and `get_historical_system_info`. The system information is computed using a weighted
    average of overloaded messages in the snapshots, with the weights being the time intervals between the snapshots.
    Each resource is computed separately, and the system is considered as overloaded whenever at least one resource
    is overloaded.

    `get_current_system_info` returns a `SystemInfo` data structure that represents the current status
    of the system. The length of the current timeframe in seconds is configurable by the `max_snapshot_age` option
    and represents the max age of snapshots to be considered for the computation.

    `SystemStatus.get_historical_system_info` returns a `SystemInfo` that represents the long-term status of the system.
    It considers the full snapshot history available in the `Snapshotter` instance.
    """

    def __init__(
        self,
        snapshotter: Snapshotter,
        *,
        max_snapshot_age: timedelta = timedelta(seconds=5),
        cpu_overload_threshold: float = 0.4,
        memory_overload_threshold: float = 0.2,
        event_loop_overload_threshold: float = 0.6,
        client_overload_threshold: float = 0.3,
    ) -> None:
        """A default constructor.

        Args:
            snapshotter: The `Snapshotter` instance to be queried for `SystemStatus`.
            max_snapshot_age: Defines max age of snapshots used in the `SystemStatus.get_current_system_info`
                measurement.
            cpu_overload_threshold: Sets the threshold of overloaded snapshots in the CPU sample.
                If the sample exceeds this threshold, the system will be considered overloaded.
            memory_overload_threshold: Sets the threshold of overloaded snapshots in the memory sample.
                If the sample exceeds this threshold, the system will be considered overloaded.
            event_loop_overload_threshold: Sets the threshold of overloaded snapshots in the event loop sample.
                If the sample exceeds this threshold, the system will be considered overloaded.
            client_overload_threshold: Sets the threshold of overloaded snapshots in the Client sample.
                If the sample exceeds this threshold, the system will be considered overloaded.
        """
        self._snapshotter = snapshotter
        self._max_snapshot_age = max_snapshot_age
        self._cpu_overload_threshold = cpu_overload_threshold
        self._memory_overload_threshold = memory_overload_threshold
        self._event_loop_overload_threshold = event_loop_overload_threshold
        self._client_overload_threshold = client_overload_threshold

    def get_current_system_info(self) -> SystemInfo:
        """Retrieves and evaluates the current status of system resources.

        Considers snapshots within the `_max_snapshot_age` timeframe and determines if the system is currently
        overloaded based on predefined thresholds for each resource type.

        Returns:
            An object representing the current system status.
        """
        return self._get_system_info(sample_duration=self._max_snapshot_age)

    def get_historical_system_info(self) -> SystemInfo:
        """Retrieves and evaluates the historical status of system resources.

        Considers the entire history of snapshots from the Snapshotter to assess long-term system performance and
        determines if the system has been historically overloaded.

        Returns:
            An object representing the historical system status.
        """
        return self._get_system_info()

    def _get_system_info(self, *, sample_duration: timedelta | None = None) -> SystemInfo:
        """Get system information based on the overload state of different resources within a specified duration.

        Args:
            sample_duration: Specific duration for which to evaluate the system status. If None, evaluates across
                the entire history available in the snapshotter.

        Returns:
            Aggregated system status indicating whether the system is idle or overloaded.
        """
        mem_info = self._is_memory_overloaded(sample_duration)
        event_loop_info = self._is_event_loop_overloaded(sample_duration)
        cpu_info = self._is_cpu_overloaded(sample_duration)
        client_info = self._is_client_overloaded(sample_duration)

        return SystemInfo(
            memory_info=mem_info,
            event_loop_info=event_loop_info,
            cpu_info=cpu_info,
            client_info=client_info,
        )

    def _is_cpu_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the CPU has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze CPU snapshots. If None, evaluates across
                the entire history available in the snapshotter.

        Returns:
            CPU load ratio information.
        """
        sample = self._snapshotter.get_cpu_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._cpu_overload_threshold)

    def _is_memory_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if memory has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze memory snapshots. If None, evaluates across
                the entire history available in the snapshotter.

        Returns:
            Memory load ratio information.
        """
        sample = self._snapshotter.get_memory_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._memory_overload_threshold)

    def _is_event_loop_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the event loop has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze event loop snapshots. If None, evaluates across
                the entire history available in the snapshotter.

        Returns:
            Event loop load ratio information.
        """
        sample = self._snapshotter.get_event_loop_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._event_loop_overload_threshold)

    def _is_client_overloaded(self, sample_duration: timedelta | None = None) -> LoadRatioInfo:
        """Determine if the client has been overloaded within a specified time duration.

        Args:
            sample_duration: The duration within which to analyze client snapshots. If None, evaluates across
                the entire history available in the snapshotter.

        Returns:
            Client load ratio information.
        """
        sample = self._snapshotter.get_client_sample(sample_duration)
        return self._is_sample_overloaded(sample, self._client_overload_threshold)

    def _is_sample_overloaded(self, sample: list[Snapshot], threshold: float) -> LoadRatioInfo:
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

        overloaded_time = 0.0
        non_overloaded_time = 0.0

        for previous, current in pairwise(sample):
            time = (current.created_at - previous.created_at).total_seconds()
            if time < 0:
                raise ValueError('Negative time. Code assumptions are not valid. Expected time sorted samples.')
            if current.is_overloaded:
                overloaded_time += time
            else:
                non_overloaded_time += time

        if (total_time := overloaded_time + non_overloaded_time) == 0:
            overloaded_ratio = 0.0
        else:
            overloaded_ratio = overloaded_time / total_time

        return LoadRatioInfo(limit_ratio=threshold, actual_ratio=round(overloaded_ratio, 3))
