# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/snapshotter.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from operator import attrgetter
from typing import TYPE_CHECKING, TypeVar, cast

from sortedcontainers import SortedList

from crawlee import service_locator
from crawlee._autoscaling._types import ClientSnapshot, CpuSnapshot, EventLoopSnapshot, MemorySnapshot, Snapshot
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_memory_info
from crawlee.events._types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.configuration import Configuration

logger = getLogger(__name__)

T = TypeVar('T')


@docs_group('Classes')
class Snapshotter:
    """Monitors and logs system resource usage at predefined intervals for performance optimization.

    The class monitors and records the state of various system resources (CPU, memory, event loop, and client API)
    at predefined intervals. This continuous monitoring helps in identifying resource overloads and ensuring optimal
    performance of the application. It is utilized in the `AutoscaledPool` module to adjust task allocation
    dynamically based on the current demand and system load.
    """

    _EVENT_LOOP_SNAPSHOT_INTERVAL = timedelta(milliseconds=500)
    """The interval at which the event loop is sampled."""

    _CLIENT_SNAPSHOT_INTERVAL = timedelta(milliseconds=1000)
    """The interval at which the client is sampled."""

    _SNAPSHOT_HISTORY = timedelta(seconds=30)
    """The time interval for which the snapshots are kept."""

    _RESERVE_MEMORY_RATIO = 0.5
    """Fraction of memory kept in reserve. Used to calculate critical memory overload threshold."""

    _MEMORY_WARNING_COOLDOWN_PERIOD = timedelta(milliseconds=10000)
    """Minimum time interval between logging successive critical memory overload warnings."""

    _CLIENT_RATE_LIMIT_ERROR_RETRY_COUNT = 2
    """Number of retries for a client request before considering it a failure due to rate limiting."""

    def __init__(
        self,
        *,
        max_used_cpu_ratio: float,
        max_used_memory_ratio: float,
        max_event_loop_delay: timedelta,
        max_client_errors: int,
        max_memory_size: ByteSize,
    ) -> None:
        """Initialize a new instance.

        In most cases, you should use the `from_config` constructor to create a new instance based on
        the provided configuration.

        Args:
            max_used_cpu_ratio: Sets the ratio, defining the maximum CPU usage. When the CPU usage is higher than
                the provided ratio, the CPU is considered overloaded.
            max_used_memory_ratio: Sets the ratio, defining the maximum ratio of memory usage. When the memory usage
                is higher than the provided ratio of `max_memory_size`, the memory is considered overloaded.
            max_event_loop_delay: Sets the maximum delay of the event loop. When the delay is higher than the provided
                value, the event loop is considered overloaded.
            max_client_errors: Sets the maximum number of client errors (HTTP 429). When the number of client errors
                is higher than the provided number, the client is considered overloaded.
            max_memory_size: Sets the maximum amount of system memory to be used by the `AutoscaledPool`.
        """
        self._max_used_cpu_ratio = max_used_cpu_ratio
        self._max_used_memory_ratio = max_used_memory_ratio
        self._max_event_loop_delay = max_event_loop_delay
        self._max_client_errors = max_client_errors
        self._max_memory_size = max_memory_size

        self._cpu_snapshots = self._get_sorted_list_by_created_at(list[CpuSnapshot]())
        self._event_loop_snapshots = self._get_sorted_list_by_created_at(list[EventLoopSnapshot]())
        self._memory_snapshots = self._get_sorted_list_by_created_at(list[MemorySnapshot]())
        self._client_snapshots = self._get_sorted_list_by_created_at(list[ClientSnapshot]())

        self._snapshot_event_loop_task = RecurringTask(self._snapshot_event_loop, self._EVENT_LOOP_SNAPSHOT_INTERVAL)
        self._snapshot_client_task = RecurringTask(self._snapshot_client, self._CLIENT_SNAPSHOT_INTERVAL)

        self._timestamp_of_last_memory_warning: datetime = datetime.now(timezone.utc) - timedelta(hours=1)

        # Flag to indicate the context state.
        self._active = False

    @classmethod
    def from_config(cls, config: Configuration | None = None) -> Snapshotter:
        """Initialize a new instance based on the provided `Configuration`.

        Args:
            config: The `Configuration` instance. Uses the global (default) one if not provided.
        """
        config = service_locator.get_configuration()

        # Compute the maximum memory size based on the provided configuration. If `memory_mbytes` is provided,
        # it uses that value. Otherwise, it calculates the `max_memory_size` as a proportion of the system's
        # total available memory based on `available_memory_ratio`.
        max_memory_size = (
            ByteSize.from_mb(config.memory_mbytes)
            if config.memory_mbytes
            else ByteSize(int(get_memory_info().total_size.bytes * config.available_memory_ratio))
        )

        return cls(
            max_used_cpu_ratio=config.max_used_cpu_ratio,
            max_used_memory_ratio=config.max_used_memory_ratio,
            max_event_loop_delay=config.max_event_loop_delay,
            max_client_errors=config.max_client_errors,
            max_memory_size=max_memory_size,
        )

    @staticmethod
    def _get_sorted_list_by_created_at(input_list: list[T]) -> SortedList[T]:
        return SortedList(input_list, key=attrgetter('created_at'))

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

    async def __aenter__(self) -> Snapshotter:
        """Start capturing snapshots at configured intervals.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True
        event_manager = service_locator.get_event_manager()
        event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        self._snapshot_event_loop_task.start()
        self._snapshot_client_task.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop all resource capturing.

        This method stops capturing snapshots of system resources (CPU, memory, event loop, and client information).
        It should be called to terminate resource capturing when it is no longer needed.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        event_manager = service_locator.get_event_manager()
        event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        await self._snapshot_event_loop_task.stop()
        await self._snapshot_client_task.stop()
        self._active = False

    @ensure_context
    def get_memory_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Return a sample of the latest memory snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of memory snapshots.
        """
        snapshots = cast('list[Snapshot]', self._memory_snapshots)
        return self._get_sample(snapshots, duration)

    @ensure_context
    def get_event_loop_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Return a sample of the latest event loop snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of event loop snapshots.
        """
        snapshots = cast('list[Snapshot]', self._event_loop_snapshots)
        return self._get_sample(snapshots, duration)

    @ensure_context
    def get_cpu_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Return a sample of the latest CPU snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of CPU snapshots.
        """
        snapshots = cast('list[Snapshot]', self._cpu_snapshots)
        return self._get_sample(snapshots, duration)

    @ensure_context
    def get_client_sample(self, duration: timedelta | None = None) -> list[Snapshot]:
        """Return a sample of the latest client snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of client snapshots.
        """
        snapshots = cast('list[Snapshot]', self._client_snapshots)
        return self._get_sample(snapshots, duration)

    @staticmethod
    def _get_sample(snapshots: list[Snapshot], duration: timedelta | None = None) -> list[Snapshot]:
        """Return a time-limited sample from snapshots or full history if duration is None."""
        if not duration:
            return snapshots

        if not snapshots:
            return []

        latest_time = snapshots[-1].created_at
        return [snapshot for snapshot in snapshots if latest_time - snapshot.created_at <= duration]

    def _snapshot_cpu(self, event_data: EventSystemInfoData) -> None:
        """Capture a snapshot of the current CPU usage.

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

        snapshots = cast('list[Snapshot]', self._cpu_snapshots)
        self._prune_snapshots(snapshots, event_data.cpu_info.created_at)
        self._cpu_snapshots.add(snapshot)

    def _snapshot_memory(self, event_data: EventSystemInfoData) -> None:
        """Capture a snapshot of the current memory usage.

        This method does not perform memory usage measurement. Instead, it just reads the data received through
        the `event_data` parameter, which is expected to be supplied by the event manager.

        Args:
            event_data: System info data from which memory usage is read.
        """
        snapshot = MemorySnapshot(
            current_size=event_data.memory_info.current_size,
            max_memory_size=self._max_memory_size,
            max_used_memory_ratio=self._max_used_memory_ratio,
            created_at=event_data.memory_info.created_at,
        )

        snapshots = cast('list[Snapshot]', self._memory_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._memory_snapshots.add(snapshot)
        self._evaluate_memory_load(event_data.memory_info.current_size, event_data.memory_info.created_at)

    def _snapshot_event_loop(self) -> None:
        """Capture a snapshot of the current event loop usage.

        This method evaluates the event loop's latency by comparing the expected time between snapshots to the actual
        time elapsed since the last snapshot. The delay in the snapshot reflects the time deviation due to event loop
        overhead - it's calculated by subtracting the expected interval between snapshots from the actual time elapsed
        since the last snapshot. If there's no previous snapshot, the delay is considered zero.
        """
        snapshot = EventLoopSnapshot(max_delay=self._max_event_loop_delay, delay=timedelta(seconds=0))
        previous_snapshot = self._event_loop_snapshots[-1] if self._event_loop_snapshots else None

        if previous_snapshot:
            event_loop_delay = snapshot.created_at - previous_snapshot.created_at - self._EVENT_LOOP_SNAPSHOT_INTERVAL
            snapshot.delay = event_loop_delay

        snapshots = cast('list[Snapshot]', self._event_loop_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._event_loop_snapshots.add(snapshot)

    def _snapshot_client(self) -> None:
        """Capture a snapshot of the current API state by checking for rate limit errors (HTTP 429).

        Only errors produced by a 2nd retry of the API call are considered for snapshotting since earlier errors may
        just be caused by a random spike in the number of requests and do not necessarily signify API overloading.
        """
        client = service_locator.get_storage_client()

        rate_limit_errors: dict[int, int] = client.get_rate_limit_errors()

        error_count = rate_limit_errors.get(self._CLIENT_RATE_LIMIT_ERROR_RETRY_COUNT, 0)
        previous_error_count = self._client_snapshots[-1].error_count if self._client_snapshots else 0
        snapshot = ClientSnapshot(
            error_count=error_count,
            new_error_count=error_count - previous_error_count,
            max_error_count=self._max_client_errors,
        )

        snapshots = cast('list[Snapshot]', self._client_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._client_snapshots.add(snapshot)

    def _prune_snapshots(self, snapshots: list[Snapshot], now: datetime) -> None:
        """Remove snapshots that are older than the `self._snapshot_history`.

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
            if now - snapshot.created_at <= self._SNAPSHOT_HISTORY:
                keep_from_index = i
                break

        # If all snapshots are old, keep_from_index will remain None, so we clear the list.
        # Otherwise, we keep only the recent snapshots.
        if keep_from_index is not None:
            del snapshots[:keep_from_index]
        else:
            snapshots.clear()

    def _evaluate_memory_load(self, current_memory_usage_size: ByteSize, snapshot_timestamp: datetime) -> None:
        """Evaluate and logs critical memory load conditions based on the system information.

        Args:
            current_memory_usage_size: The current memory usage.
            snapshot_timestamp: The time at which the memory snapshot was taken.
        """
        # Check if the warning has been logged recently to avoid spamming
        if snapshot_timestamp < self._timestamp_of_last_memory_warning + self._MEMORY_WARNING_COOLDOWN_PERIOD:
            return

        threshold_memory_size = self._max_used_memory_ratio * self._max_memory_size
        buffer_memory_size = self._max_memory_size * (1 - self._max_used_memory_ratio) * self._RESERVE_MEMORY_RATIO
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
