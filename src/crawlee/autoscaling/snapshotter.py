# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/snapshotter.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, cast

import psutil

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.recurring_task import RecurringTask
from crawlee.autoscaling.types import ClientSnapshot, CpuSnapshot, EventLoopSnapshot, MemorySnapshot, Snapshot
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

logger = getLogger(__name__)


class Snapshotter:
    """Monitors and logs system resource usage at predefined intervals for performance optimization.

    The class monitors and records the state of various system resources (CPU, memory, event loop, and client API)
    at predefined intervals. This continuous monitoring helps in identifying resource overloads and ensuring optimal
    performance of the application. It is utilized in the `AutoscaledPool` module to adjust task allocation
    dynamically based on the current demand and system load.
    """

    def __init__(
        self,
        event_manager: EventManager,
        *,
        event_loop_snapshot_interval: timedelta = timedelta(milliseconds=500),
        client_snapshot_interval: timedelta = timedelta(milliseconds=1000),
        max_used_cpu_ratio: float = 0.95,
        max_memory_size: ByteSize | None = None,
        max_used_memory_ratio: float = 0.7,
        max_event_loop_delay: timedelta = timedelta(milliseconds=50),
        max_client_errors: int = 1,
        snapshot_history: timedelta = timedelta(seconds=30),
        reserve_memory_ratio: float = 0.5,
        memory_warning_cooldown_period: timedelta = timedelta(milliseconds=10000),
        client_rate_limit_error_retry_count: int = 2,
    ) -> None:
        """Creates a new instance.

        Args:
            event_manager: The event manager used to emit system info events. From data provided by this event
              the CPU and memory usage are read.

            event_loop_snapshot_interval: The interval at which the event loop is sampled.

            client_snapshot_interval: The interval at which the client is sampled.

            max_used_cpu_ratio: Sets the ratio, defining the maximum CPU usage. When the CPU usage is higher than
                the provided ratio, the CPU is considered overloaded.

            max_memory_size: Sets the maximum amount of system memory to be used by the `AutoscaledPool`. If `None`
                is provided, the max amount of memory to be used is set to one quarter of total system memory.
                I.e. on a system with 8192 MB, the `AutoscaledPool` will only use up to 2048 MB of memory.

            max_used_memory_ratio: Sets the ratio, defining the maximum ratio of memory usage. When the memory usage
                is higher than the provided ratio of `max_memory_size`, the memory is considered overloaded.

            max_event_loop_delay: Sets the maximum delay of the event loop. When the delay is higher than the provided
                value, the event loop is considered overloaded.

            max_client_errors: Sets the maximum number of client errors (HTTP 429). When the number of client errors
                is higher than the provided number, the client is considered overloaded.

            snapshot_history: Sets the time interval for which the snapshots are kept.

            reserve_memory_ratio: Fraction of memory kept in reserve. Used to calculate critical memory overload
                threshold.

            memory_warning_cooldown_period: Minimum time interval between logging successive critical memory overload
                warnings.

            client_rate_limit_error_retry_count: Number of retries for a client request before considering it a failure
                due to rate limiting.
        """
        self._event_manager = event_manager
        self._event_loop_snapshot_interval = event_loop_snapshot_interval
        self._client_snapshot_interval = client_snapshot_interval
        self._max_event_loop_delay = max_event_loop_delay
        self._max_used_cpu_ratio = max_used_cpu_ratio
        self._max_used_memory_ratio = max_used_memory_ratio
        self._max_client_errors = max_client_errors
        self._snapshot_history = snapshot_history
        self._reserve_memory_ratio = reserve_memory_ratio
        self._memory_warning_cooldown_period = memory_warning_cooldown_period
        self._client_rate_limit_error_retry_count = client_rate_limit_error_retry_count
        self._max_memory_size = max_memory_size or self._get_default_max_memory_size()

        self._cpu_snapshots: list[CpuSnapshot] = []
        self._event_loop_snapshots: list[EventLoopSnapshot] = []
        self._memory_snapshots: list[MemorySnapshot] = []
        self._client_snapshots: list[ClientSnapshot] = []

        self._snapshot_event_loop_task = RecurringTask(self._snapshot_event_loop, self._event_loop_snapshot_interval)
        self._snapshot_client_task = RecurringTask(self._snapshot_client, self._client_snapshot_interval)

        self._timestamp_of_last_memory_warning: datetime = datetime.now(timezone.utc) - timedelta(hours=1)

    @staticmethod
    def _get_default_max_memory_size() -> ByteSize:
        """Default `memory_max_size` is 1/4 of the total system memory."""
        max_memory_size_in_bytes = int(psutil.virtual_memory().total * 0.25)
        max_memory_size = ByteSize(max_memory_size_in_bytes)
        logger.info(f'Setting max_memory_size of this run to {max_memory_size}.')
        return max_memory_size

    async def __aenter__(self) -> Snapshotter:
        """Starts capturing snapshots at configured intervals."""
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        self._snapshot_event_loop_task.start()
        self._snapshot_client_task.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stops all resource capturing.

        This method stops capturing snapshots of system resources (CPU, memory, event loop, and client information).
        It should be called to terminate resource capturing when it is no longer needed.
        """
        self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        await self._snapshot_event_loop_task.stop()
        await self._snapshot_client_task.stop()

    def get_memory_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest memory snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of memory snapshots.
        """
        snapshots = cast(list[Snapshot], self._memory_snapshots)
        return self._get_sample(snapshots, duration)

    def get_event_loop_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest event loop snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of event loop snapshots.
        """
        snapshots = cast(list[Snapshot], self._event_loop_snapshots)
        return self._get_sample(snapshots, duration)

    def get_cpu_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest CPU snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of CPU snapshots.
        """
        snapshots = cast(list[Snapshot], self._cpu_snapshots)
        return self._get_sample(snapshots, duration)

    def get_client_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest client snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of client snapshots.
        """
        snapshots = cast(list[Snapshot], self._client_snapshots)
        return self._get_sample(snapshots, duration)

    @staticmethod
    def _get_sample(snapshots: list[Snapshot], duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a time-limited sample from snapshots or full history if duration is None."""
        if not duration:
            return snapshots

        if not snapshots:
            return []

        latest_time = snapshots[-1].created_at
        return [snapshot for snapshot in reversed(snapshots) if latest_time - snapshot.created_at <= duration]

    def _snapshot_cpu(self, event_data: EventSystemInfoData) -> None:
        """Captures a snapshot of the current CPU usage.

        This method does not perform CPU usage measurement. Instead, it just reads the data received through
        the `event_data` parameter, which is expected to be supplied by the event manager.

        Args:
            event_data: System info data from which CPU usage is read.
        """
        snapshot = CpuSnapshot(
            used_ratio=event_data.cpu_info.used_ratio,
            max_used_ratio=self._max_used_cpu_ratio,
            created_at=event_data.cpu_info.created_at,
        )

        snapshots = cast(list[Snapshot], self._cpu_snapshots)
        self._prune_snapshots(snapshots, event_data.cpu_info.created_at)
        self._cpu_snapshots.append(snapshot)

    def _snapshot_memory(self, event_data: EventSystemInfoData) -> None:
        """Captures a snapshot of the current memory usage.

        This method does not perform memory usage measurement. Instead, it just reads the data received through
        the `event_data` parameter, which is expected to be supplied by the event manager.

        Args:
            event_data: System info data from which memory usage is read.
        """
        snapshot = MemorySnapshot(
            total_size=event_data.memory_info.total_size,
            current_size=event_data.memory_info.current_size,
            max_memory_size=self._max_memory_size,
            max_used_memory_ratio=self._max_used_memory_ratio,
            created_at=event_data.memory_info.created_at,
        )

        snapshots = cast(list[Snapshot], self._memory_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._memory_snapshots.append(snapshot)

        self._evaluate_memory_load(event_data.memory_info.current_size, event_data.memory_info.created_at)

    def _snapshot_event_loop(self) -> None:
        """Captures a snapshot of the current event loop usage.

        This method evaluates the event loop's latency by comparing the expected time between snapshots to the actual
        time elapsed since the last snapshot. The delay in the snapshot reflects the time deviation due to event loop
        overhead - it's calculated by subtracting the expected interval between snapshots from the actual time elapsed
        since the last snapshot. If there's no previous snapshot, the delay is considered zero.
        """
        snapshot = EventLoopSnapshot(max_delay=self._max_event_loop_delay, delay=timedelta(seconds=0))
        previous_snapshot = self._event_loop_snapshots[-1] if self._event_loop_snapshots else None

        if previous_snapshot:
            event_loop_delay = snapshot.created_at - previous_snapshot.created_at - self._event_loop_snapshot_interval
            snapshot.delay = event_loop_delay

        snapshots = cast(list[Snapshot], self._event_loop_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._event_loop_snapshots.append(snapshot)

    def _snapshot_client(self) -> None:
        """Captures a snapshot of the current API state by checking for rate limit errors (HTTP 429).

        Only errors produced by a 2nd retry of the API call are considered for snapshotting since earlier errors may
        just be caused by a random spike in the number of requests and do not necessarily signify API overloading.
        """
        # TODO: This is just a dummy placeholder. It can be implemented once `StorageClient` is ready.
        # Attribute `self._client_rate_limit_error_retry_count` will be used here.
        # https://github.com/apify/crawlee-py/issues/60

        error_count = 0
        snapshot = ClientSnapshot(error_count=error_count, max_error_count=self._max_client_errors)

        snapshots = cast(list[Snapshot], self._client_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._client_snapshots.append(snapshot)

    def _prune_snapshots(self, snapshots: list[Snapshot], now: datetime) -> None:
        """Removes snapshots that are older than the `self._snapshot_history`.

        This method modifies the list of snapshots in place, removing all snapshots that are older than the defined
        snapshot history relative to the `now` parameter.

        Args:
            snapshots: List of snapshots to be pruned in place.
            now: The current date and time, used as the reference for pruning.
        """
        # Find the index where snapshots start to be within the allowed history window.
        # We'll keep snapshots from this index onwards.
        keep_from_index = None
        for i, snapshot in enumerate(snapshots):
            if now - snapshot.created_at <= self._snapshot_history:
                keep_from_index = i
                break

        # If all snapshots are old, keep_from_index will remain None, so we clear the list.
        # Otherwise, we keep only the recent snapshots.
        if keep_from_index is not None:
            del snapshots[:keep_from_index]
        else:
            snapshots.clear()

    def _evaluate_memory_load(self, current_memory_usage_size: ByteSize, snapshot_timestamp: datetime) -> None:
        """Evaluates and logs critical memory load conditions based on the system information.

        Args:
            current_memory_usage_size: The current memory usage.
            snapshot_timestamp: The time at which the memory snapshot was taken.
        """
        # Check if the warning has been logged recently to avoid spamming
        if snapshot_timestamp < self._timestamp_of_last_memory_warning + self._memory_warning_cooldown_period:
            return

        threshold_memory_size = self._max_used_memory_ratio * self._max_memory_size
        buffer_memory_size = self._max_memory_size * (1 - self._max_used_memory_ratio) * self._reserve_memory_ratio
        overload_memory_threshold_size = threshold_memory_size + buffer_memory_size

        # Log a warning if current memory usage exceeds the critical overload threshold
        if current_memory_usage_size > overload_memory_threshold_size:
            memory_usage_percentage = round((current_memory_usage_size.bytes / self._max_memory_size.bytes) * 100)
            logger.warning(
                f'Memory is critically overloaded. Using {current_memory_usage_size} of '
                f'{self._max_memory_size} ({memory_usage_percentage}%). '
                'Consider increasing available memory.'
            )
            self._timestamp_of_last_memory_warning = snapshot_timestamp
