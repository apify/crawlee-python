# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/snapshotter.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, cast

from crawlee._utils.math import to_mb
from crawlee._utils.recurring_task import RecurringTask
from crawlee._utils.system import get_memory_info
from crawlee.autoscaling.types import ClientSnapshot, CpuSnapshot, EventLoopSnapshot, MemorySnapshot, Snapshot
from crawlee.events.types import Event, EventSystemInfoData

if TYPE_CHECKING:
    from crawlee.autoscaling.system_status import SystemInfo
    from crawlee.events import EventManager

logger = getLogger(__name__)


class Snapshotter:
    """Creates snapshots of system resources at given intervals.

    Creates snapshots of system resources at given intervals and marks the resource as either overloaded or not during
    the last interval. Keeps a history of the snapshots. It tracks the following resources: Memory, EventLoop, API
    and CPU. The class is used by the `AutoscaledPool` class.

    When running on the Apify platform, the CPU and memory statistics are provided by the platform, as collected from
    the running Docker container. When running locally, `Snapshotter` makes its own statistics by querying the OS.

    CPU becomes overloaded locally when its current use exceeds the `maxUsedCpuRatio` option or when Apify platform
    marks it as overloaded.

    Memory becomes overloaded if its current use exceeds the `max_used_memory_ratio` option. It's computed using
    the total memory available to the container when running on the Apify platform and a quarter of total system
    memory when running locally. Max total memory when running locally may be overridden by using
    the `CRAWLEE_MEMORY_MBYTES` environment variable.

    Event loop becomes overloaded if it slows down by more than the `max_blocked` option.

    Client becomes overloaded when rate limit errors (429 - Too Many Requests) exceeds the `max_client_errors` option,
    typically received from the request queue, exceed the set limit within the set interval.
    """

    def __init__(
        self: Snapshotter,
        *,
        event_manager: EventManager,
        event_loop_snapshot_interval: timedelta = timedelta(milliseconds=500),
        client_snapshot_interval: timedelta = timedelta(milliseconds=1000),
        max_used_cpu_ratio: float = 0.95,
        max_memory_bytes: int | None = None,
        max_used_memory_ratio: float = 0.7,
        max_event_loop_delay: timedelta = timedelta(milliseconds=50),
        max_client_errors: int = 1,
        snapshot_history: timedelta = timedelta(seconds=30),
        reserve_memory_ratio: float = 0.5,
        client_rate_limit_error_retry_count: int = 2,
        critical_overload_rate_limit: timedelta = timedelta(milliseconds=10000),
    ) -> None:
        """Initializes the Snapshotter.

        Args:
            event_manager: The event manager used to emit system info events. From data provided by this event
              the CPU and memory usage are read.

            event_loop_snapshot_interval: The interval at which the event loop is sampled.

            client_snapshot_interval: The interval at which the client is sampled.

            max_used_cpu_ratio: Sets the ratio, defining the maximum CPU usage. When the CPU usage is higher than
                the provided ratio, the CPU is considered overloaded.

            max_memory_bytes: Sets the maximum amount of system memory to be used by the `AutoscaledPool`. If `None`
                is provided, the max amount of memory to be used is set to one quarter of total system memory.
                I.e. on a system with 8192 MB, the `AutoscaledPool` will only use up to 2048 MB of memory.

            max_used_memory_ratio: Sets the ratio, defining the maximum ratio of memory usage. When the memory usage
                is higher than the provided ratio of `max_memory_bytes`, the memory is considered overloaded.

            max_event_loop_delay: Sets the maximum delay of the event loop. When the delay is higher than the provided
                value, the event loop is considered overloaded.

            max_client_errors: Sets the maximum number of client errors (HTTP 429). When the number of client errors
                is higher than the provided number, the client is considered overloaded.

            snapshot_history: Sets the time interval for which the snapshots are kept.

            reserve_memory_ratio: TODO

            client_rate_limit_error_retry_count: TODO

            critical_overload_rate_limit: TODO
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
        self._client_rate_limit_error_retry_count = client_rate_limit_error_retry_count
        self._critical_overload_rate_limit = critical_overload_rate_limit

        # Default `memory_max_bytes`` is 1/4 of the total system memory
        if max_memory_bytes is None:
            memory_info = get_memory_info()
            self._max_memory_bytes = int(memory_info.total_bytes * 0.25)
            logger.debug(f'Setting max_memory_bytes of this run to {round(self._max_memory_bytes / 1024 / 1024)} MB.')
        else:
            self._max_memory_bytes = max_memory_bytes

        # Post initialization
        self._cpu_snapshots: list[CpuSnapshot] = []
        self._event_loop_snapshots: list[EventLoopSnapshot] = []
        self._memory_snapshots: list[MemorySnapshot] = []
        self._client_snapshots: list[ClientSnapshot] = []

        self._snapshot_event_loop_task = RecurringTask(self._snapshot_event_loop, self._event_loop_snapshot_interval)
        self._snapshot_client_task = RecurringTask(self._snapshot_client, self._client_snapshot_interval)
        self._last_logged_critical_memory_overload_at = None

    async def start(self: Snapshotter) -> None:
        """Starts capturing snapshots at configured intervals."""
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self._event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        self._snapshot_event_loop_task.start()
        self._snapshot_client_task.start()

    async def stop(self: Snapshotter) -> None:
        """Stops all resource capturing.

        This method stops capturing snapshots of system resources (CPU, memory, event loop, and client information).
        It should be called to terminate resource capturing when it is no longer needed.
        """
        self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self._event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)
        await self._snapshot_event_loop_task.stop()
        await self._snapshot_client_task.stop()

    def get_memory_sample(self: Snapshotter, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest memory snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of memory snapshots.
        """
        snapshots = cast(list[Snapshot], self._memory_snapshots)
        return self._get_sample(snapshots, duration)

    def get_event_loop_sample(self: Snapshotter, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest event loop snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of event loop snapshots.
        """
        snapshots = cast(list[Snapshot], self._event_loop_snapshots)
        return self._get_sample(snapshots, duration)

    def get_cpu_sample(self: Snapshotter, duration: timedelta | None = None) -> list[Snapshot]:
        """Returns a sample of the latest CPU snapshots.

        Args:
            duration: The duration of the sample from the latest snapshot. If omitted, it returns a full history.

        Returns:
            A sample of CPU snapshots.
        """
        snapshots = cast(list[Snapshot], self._cpu_snapshots)
        return self._get_sample(snapshots, duration)

    def get_client_sample(self: Snapshotter, duration: timedelta | None = None) -> list[Snapshot]:
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

    def _snapshot_cpu(self: Snapshotter, event_data: EventSystemInfoData) -> None:
        """Creates a snapshot of current CPU usage using the Apify platform `SystemInfo` event.

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

    def _snapshot_memory(self: Snapshotter, event_data: EventSystemInfoData) -> None:
        """Creates a snapshot of current memory usage using the Apify platform `SystemInfo` event.

        This method does not perform memory usage measurement. Instead, it just reads the data received through
        the `event_data` parameter, which is expected to be supplied by the event manager.

        Args:
            event_data: System info data from which memory usage is read.
        """
        snapshot = MemorySnapshot(
            total_bytes=event_data.memory_info.total_bytes,
            current_bytes=event_data.memory_info.current_bytes,
            max_memory_bytes=self._max_memory_bytes,
            max_used_memory_ratio=self._max_used_memory_ratio,
            created_at=event_data.memory_info.created_at,
        )

        snapshots = cast(list[Snapshot], self._memory_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._memory_snapshots.append(snapshot)

    def _snapshot_event_loop(self: Snapshotter) -> None:
        """Creates a snapshot of the current event loop state.

        This method assesses the event loop's responsiveness by measuring the time elapsed between the creation of
        the last snapshot and the current moment. This delay is calculated by subtracting the time of the last snapshot
        and the predefined snapshot interval from the current time. If this is the first snapshot or no previous
        snapshots exist, the delay is set to zero.
        """
        snapshot = EventLoopSnapshot(max_delay=self._max_event_loop_delay, delay=timedelta(seconds=0))
        previous_snapshot = self._event_loop_snapshots[-1] if self._event_loop_snapshots else None

        if previous_snapshot:
            event_loop_delay = snapshot.created_at - previous_snapshot.created_at - self._event_loop_snapshot_interval
            snapshot.delay = event_loop_delay

        snapshots = cast(list[Snapshot], self._event_loop_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._event_loop_snapshots.append(snapshot)

    def _snapshot_client(self: Snapshotter) -> None:
        """Creates a snapshot of the current API state by checking for rate limit errors (HTTP 429).

        Only errors produced by a 2nd retry of the API call are considered for snapshotting since earlier errors may
        just be caused by a random spike in the number of requests and do not necessarily signify API overloading.
        """
        # TODO: This is just a dummy placeholder. It can be implemented once `StorageClient` is ready.
        # https://github.com/apify/crawlee-py/issues/60

        num_of_errors = 0
        snapshot = ClientSnapshot(num_of_errors=num_of_errors, max_num_of_errors=self._max_client_errors)

        snapshots = cast(list[Snapshot], self._client_snapshots)
        self._prune_snapshots(snapshots, snapshot.created_at)
        self._client_snapshots.append(snapshot)

    def _prune_snapshots(self: Snapshotter, snapshots: list[Snapshot], now: datetime) -> None:
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

    def _memory_overload_warning(self: Snapshotter, system_info: SystemInfo) -> None:
        """Checks for critical memory overload and logs it to the console.

        Args:
            system_info: System info
        """
        # TODO
        mem_current_bytes = system_info.mem_current_bytes
        created_at = system_info.created_at or datetime.now(tz=timezone.utc)
        critical_overload_rate_limit_millis = self._critical_overload_rate_limit.total_seconds() * 1000

        if (
            self._last_logged_critical_memory_overload_at
            and created_at < self._last_logged_critical_memory_overload_at + critical_overload_rate_limit_millis
        ):
            return

        max_desired_memory_bytes = self._max_used_memory_ratio * self._max_memory_bytes
        reserve_memory = self._max_memory_bytes * (1 - self._max_used_memory_ratio) * self._reserve_memory_ratio
        critical_overload_bytes = max_desired_memory_bytes + reserve_memory

        if mem_current_bytes is None:
            raise ValueError('mem_current_bytes is None')

        is_critical_overload = mem_current_bytes > critical_overload_bytes

        if is_critical_overload:
            used_percentage = round((mem_current_bytes / self._max_memory_bytes) * 100)

            logger.warning(
                f'Memory is critically overloaded. Using {to_mb(mem_current_bytes)} MB of '
                f'{to_mb(self._max_memory_bytes)} MB ({used_percentage}%). Consider increasing available memory.'
            )
