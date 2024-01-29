from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Sequence

from more_itertools import pairwise

from crawlee.utils import weighted_avg

if TYPE_CHECKING:
    from datetime import datetime

    from crawlee.autoscaling.snapshotter import Snapshot, Snapshotter

logger = getLogger(__name__)


@dataclass
class LoadRatioInfo:
    """Represents the load ratio of a resource."""

    is_overloaded: bool
    limit_ratio: float
    actual_ratio: float


@dataclass
class SystemInfo:
    """Represents the current status of the system."""

    is_system_idle: bool  # Indicates whether the system is currently idle or overloaded
    mem_info: LoadRatioInfo
    event_loop_info: LoadRatioInfo
    cpu_info: LoadRatioInfo
    client_info: LoadRatioInfo
    mem_current_bytes: int | None = None  # Platform only property
    cpu_current_usage: int | None = None  # Platform only property
    is_cpu_overloaded: bool | None = None  # Platform only property
    created_at: datetime | None = None


@dataclass
class FinalStatistics:
    """Represents final statistics."""

    requests_finished: int
    requests_failed: int
    retry_histogram: list[int]
    request_avg_failed_duration_millis: float
    request_avg_finished_duration_millis: float
    requests_finished_per_minute: float
    requests_failed_per_minute: float
    request_total_duration_millis: float
    requests_total: int
    crawler_runtime_millis: float


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

    Attributes:
        snapshotter: The `Snapshotter` instance to be queried for `SystemStatus`.

        current_history_secs: Defines max age of snapshots used in the `SystemStatus.get_current_status` measurement.

        max_memory_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a memory sample.
            If the sample exceeds this ratio, the system will be overloaded.

        max_event_loop_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in an event loop sample.
            If the sample exceeds this ratio, the system will be overloaded.

        max_cpu_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a CPU sample. If the sample
            exceeds this ratio, the system will be overloaded.

        max_client_overloaded_ratio: Sets the maximum ratio of overloaded snapshots in a Client sample.
            If the sample exceeds this ratio, the system will be overloaded.
    """

    def __init__(
        self: SystemStatus,
        snapshotter: Snapshotter,
        current_history_secs: int = 5,
        max_memory_overloaded_ratio: float = 0.2,
        max_event_loop_overloaded_ratio: float = 0.6,
        max_cpu_overloaded_ratio: float = 0.4,
        max_client_overloaded_ratio: float = 0.3,
    ) -> None:
        self.snapshotter = snapshotter
        self.current_history_secs = current_history_secs
        self.max_memory_overloaded_ratio = max_memory_overloaded_ratio
        self.max_event_loop_overloaded_ratio = max_event_loop_overloaded_ratio
        self.max_cpu_overloaded_ratio = max_cpu_overloaded_ratio
        self.max_client_overloaded_ratio = max_client_overloaded_ratio

    def get_current_status(self: SystemStatus) -> SystemInfo:
        """Get the current system status.

        Returns a `SystemInfo` object where the `is_system_idle` property is `False` if the system has been overloaded
        in the last `current_history_secs` seconds, and `True` otherwise.

        Returns:
            An object representing the current system status.
        """
        return self._is_system_idle(self.current_history_secs)

    def get_historical_status(self: SystemStatus) -> SystemInfo:
        """Get the historical system status.

        Returns a `SystemInfo` where the `is_system_idle` property is set to `False` if the system has been overloaded
        in the full history of the `Snapshotter` (which is configurable in the `Snapshotter`, and `True` otherwise.

        Returns:
            An object representing the historical system status.
        """
        return self._is_system_idle()

    def _is_system_idle(self: SystemStatus, sample_duration_millis: int | None = None) -> SystemInfo:
        """Determine if the system is currently idle or overloaded.

        Args:
            sample_duration_millis: The duration (in milliseconds) within which to analyze system status.

        Returns:
            An object representing the system status with an `is_system_idle` property set to `True` if the system
            has not been overloaded within the specified time duration, and `False` otherwise.
        """
        mem_info = self._is_memory_overloaded(sample_duration_millis)
        event_loop_info = self._is_event_loop_overloaded(sample_duration_millis)
        cpu_info = self._is_cpu_overloaded(sample_duration_millis)
        client_info = self._is_client_overloaded(sample_duration_millis)

        return SystemInfo(
            is_system_idle=(
                not mem_info.is_overloaded
                and not event_loop_info.is_overloaded
                and not cpu_info.is_overloaded
                and not client_info.is_overloaded
            ),
            mem_info=mem_info,
            event_loop_info=event_loop_info,
            cpu_info=cpu_info,
            client_info=client_info,
        )

    def _is_memory_overloaded(self: SystemStatus, sample_duration_millis: int | None = None) -> LoadRatioInfo:
        """Determine if memory has been overloaded within a specified time duration.

        Args:
            sample_duration_millis: The duration (in milliseconds) within which to analyze memory snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if memory has been overloaded within the specified
            time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self.snapshotter.get_memory_sample(sample_duration_millis)
        return self._is_sample_overloaded(sample, self.max_memory_overloaded_ratio)

    def _is_event_loop_overloaded(self: SystemStatus, sample_duration_millis: int | None = None) -> LoadRatioInfo:
        """Determine if the event loop has been overloaded within a specified time duration.

        Args:
            sample_duration_millis: The duration (in milliseconds) within which to analyze event loop snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the event loop has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self.snapshotter.get_event_loop_sample(sample_duration_millis)
        return self._is_sample_overloaded(sample, self.max_event_loop_overloaded_ratio)

    def _is_cpu_overloaded(self: SystemStatus, sample_duration_millis: int | None = None) -> LoadRatioInfo:
        """Determine if the CPU has been overloaded within a specified time duration.

        Args:
            sample_duration_millis: The duration (in milliseconds) within which to analyze CPU snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the CPU has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self.snapshotter.get_cpu_sample(sample_duration_millis)
        return self._is_sample_overloaded(sample, self.max_cpu_overloaded_ratio)

    def _is_client_overloaded(self: SystemStatus, sample_duration_millis: int | None = None) -> LoadRatioInfo:
        """Determine if the client has been overloaded within a specified time duration.

        Args:
            sample_duration_millis: The duration (in milliseconds) within which to analyze client snapshots.

        Returns:
            An object with an `is_overloaded` property set to `True` if the client has been overloaded within
            the specified time duration. Otherwise, `is_overloaded` is set to `False`.
        """
        sample = self.snapshotter.get_client_sample(sample_duration_millis)
        return self._is_sample_overloaded(sample, self.max_client_overloaded_ratio)

    def _is_sample_overloaded(self: SystemStatus, sample: Sequence[Snapshot], ratio: float) -> LoadRatioInfo:
        """Determine if a sample of snapshot data is overloaded based on a specified ratio.

        Args:
            sample: A sequence of snapshot data to analyze.
            ratio: The ratio threshold to consider the sample as overloaded.

        Returns:
            An object with an `is_overloaded` property set to `True` if the sample is considered overloaded based
            on the specified ratio. Otherwise, `is_overloaded` is set to `False`.
        """
        if not sample:
            return LoadRatioInfo(is_overloaded=False, limit_ratio=ratio, actual_ratio=0)

        weights, values = [], []

        for previous, current in pairwise(sample):
            weight = (current.created_at - previous.created_at).total_seconds() or 0.001  # Avoid zero
            weights.append(weight)
            values.append(float(current.is_overloaded))

        w_avg = values[0] if len(sample) == 1 else weighted_avg(values, weights)

        return LoadRatioInfo(
            is_overloaded=w_avg > ratio,
            limit_ratio=ratio,
            actual_ratio=round(w_avg, 3),
        )
