# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/snapshotter.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Sequence

from crawlee._utils.math import to_mb
from crawlee._utils.recurring_task import RecurringTask
from crawlee.autoscaling.memory_info import MemoryInfo, get_memory_info
from crawlee.autoscaling.types import ClientSnapshot, CpuSnapshot, EventLoopSnapshot, MemorySnapshot
from crawlee.events import EventManager
from crawlee.events.types import Event

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee import Config
    from crawlee.autoscaling.system_status import SystemInfo
    from crawlee.autoscaling.types import Snapshot

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

    Attributes:
        config: ...
        event_loop_snapshot_interval_millis: ...
        client_snapshot_interval_millis: ...
        max_blocked_millis: ...
        max_used_memory_ratio: ...
        max_client_errors: ...
        snapshot_history_millis: ...
        event_manager: ...
        max_memory_bytes: ...
        cpu_snapshots: ...
        event_loop_snapshots: ...
        memory_snapshots: ...
        client_snapshots: ...
        snapshot_event_loop_task: ...
        snapshot_client_task: ...
        last_logged_critical_memory_overload_at: ...
    """

    def __init__(
        self: Snapshotter,
        config: Config,
        *,
        event_loop_snapshot_interval: timedelta = timedelta(milliseconds=500),
        client_snapshot_interval: timedelta = timedelta(milliseconds=1000),
        max_blocked: timedelta = timedelta(milliseconds=30),
        max_used_memory_ratio: float = 0.7,
        max_client_errors: int = 3,
        snapshot_history: timedelta = timedelta(seconds=30),
        reserve_memory_ratio: float = 0.5,
        client_rate_limit_error_retry_count: int = 2,
        critical_overload_rate_limit: timedelta = timedelta(milliseconds=10000),
    ) -> None:
        self.config = config
        self.event_loop_snapshot_interval = event_loop_snapshot_interval
        self.client_snapshot_interval = client_snapshot_interval
        self.max_blocked = max_blocked
        self.max_used_memory_ratio = max_used_memory_ratio
        self.max_client_errors = max_client_errors
        self.snapshot_history = snapshot_history
        self.reserve_memory_ratio = reserve_memory_ratio
        self.client_rate_limit_error_retry_count = client_rate_limit_error_retry_count
        self.critical_overload_rate_limit = critical_overload_rate_limit

        self.event_manager = EventManager()

        self.max_memory_bytes = 0

        self.cpu_snapshots: list[CpuSnapshot] = []
        self.event_loop_snapshots: list[EventLoopSnapshot] = []
        self.memory_snapshots: list[MemorySnapshot] = []
        self.client_snapshots: list[ClientSnapshot] = []

        self.snapshot_event_loop_task = RecurringTask(self._snapshot_event_loop, self.event_loop_snapshot_interval)

        self.snapshot_client_task = RecurringTask(self._snapshot_client, self.client_snapshot_interval)

        self.last_logged_critical_memory_overload_at = None

    async def start(self: Snapshotter) -> None:
        """Starts capturing snapshots at configured intervals."""
        memory_mbytes = self.config.memory_mbytes

        if memory_mbytes > 0:
            self.max_memory_bytes = memory_mbytes * 1024 * 1024
        else:
            memory_info = await self._get_memory_info()
            total_bytes = memory_info.total_bytes
            # TODO:
            # self.max_memory_bytes = int(total_bytes * self.config.available_memory_ratio)
            self.max_memory_bytes = int(total_bytes * 0.25)
            logger.debug(
                f'Setting max memory of this run to {round(self.max_memory_bytes / 1024 / 1024)} MB. '
                'Use the CRAWLEE_MEMORY_MBYTES or CRAWLEE_AVAILABLE_MEMORY_RATIO environment variable to override it.'
            )

        self.snapshot_event_loop_task.start()
        self.snapshot_client_task.start()
        self.event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self.event_manager.on(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)

    async def stop(self: Snapshotter) -> None:
        """Stops all resource capturing.

        This method stops capturing snapshots of system resources such as CPU, memory, event loop,
        and client information. It should be called to terminate resource capturing when it is no longer needed.
        """
        await self.snapshot_event_loop_task.stop()
        await self.snapshot_client_task.stop()
        self.event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_cpu)
        self.event_manager.off(event=Event.SYSTEM_INFO, listener=self._snapshot_memory)

    def get_memory_sample(self: Snapshotter, sample_duration: timedelta | None = None) -> Sequence[Snapshot]:
        """Returns a sample of the latest memory snapshots.

        Args:
            sample_duration: The size of the sample. If omitted, it returns a full snapshot history.

        Returns:
            A sample of memory snapshots.
        """
        return self._get_sample(self.memory_snapshots, sample_duration)

    def get_event_loop_sample(self: Snapshotter, sample_duration: timedelta | None = None) -> Sequence[Snapshot]:
        """Returns a sample of the latest event loop snapshots.

        Args:
            sample_duration: The size of the sample. If omitted, it returns a full snapshot history.

        Returns:
            A sample of event loop snapshots.
        """
        return self._get_sample(self.event_loop_snapshots, sample_duration)

    def get_cpu_sample(self: Snapshotter, sample_duration: timedelta | None = None) -> Sequence[Snapshot]:
        """Returns a sample of the latest CPU snapshots.

        Args:
            sample_duration: The size of the sample. If omitted, it returns a full snapshot history.

        Returns:
            A sample of CPU snapshots.
        """
        return self._get_sample(self.cpu_snapshots, sample_duration)

    def get_client_sample(self: Snapshotter, sample_duration: timedelta | None = None) -> Sequence[Snapshot]:
        """Returns a sample of the latest client snapshots.

        Args:
            sample_duration: The size of the sample. If omitted, it returns a full snapshot history.

        Returns:
            A sample of client snapshots.
        """
        return self._get_sample(self.client_snapshots, sample_duration)

    def _get_sample(
        self: Snapshotter,
        snapshots: Sequence[Snapshot],
        sample_duration: timedelta | None = None,
    ) -> Sequence[Snapshot]:
        """Finds the latest snapshots by sample_duration in the provided Sequence.

        Args:
            snapshots: Sequence of snapshots
            sample_duration: The size of the sample. If omitted, it returns a full snapshot history.

        Returns:
            A sample of snapshots.
        """
        if not sample_duration:
            return snapshots

        sample: list[Snapshot] = []
        idx = len(snapshots)
        if not idx:
            return sample

        latest_time = snapshots[idx - 1].created_at
        while idx:
            snapshot = snapshots[idx - 1]
            if latest_time - snapshot.created_at <= sample_duration:
                sample.insert(0, snapshot)
            else:
                break

        return sample

    def _snapshot_memory(self: Snapshotter, system_info: SystemInfo) -> None:
        """Creates a snapshot of current memory usage using the Apify platform `systemInfo` event.

        Args:
            system_info: System info
        """
        created_at = system_info.created_at or datetime.now(tz=timezone.utc)
        mem_current_bytes = system_info.mem_current_bytes

        if mem_current_bytes is None:
            raise ValueError('mem_current_bytes is None')

        snapshot = MemorySnapshot(
            created_at=created_at,
            is_overloaded=mem_current_bytes / self.max_memory_bytes > self.max_used_memory_ratio,
            used_bytes=mem_current_bytes,
        )

        self._prune_snapshots(self.memory_snapshots, created_at)

        self.memory_snapshots.append(snapshot)
        self._memory_overload_warning(system_info)

    def _memory_overload_warning(self: Snapshotter, system_info: SystemInfo) -> None:
        """Checks for critical memory overload and logs it to the console.

        Args:
            system_info: System info
        """
        mem_current_bytes = system_info.mem_current_bytes
        created_at = system_info.created_at or datetime.now(tz=timezone.utc)
        critical_overload_rate_limit_millis = self.critical_overload_rate_limit.total_seconds() * 1000

        if (
            self.last_logged_critical_memory_overload_at
            and created_at < self.last_logged_critical_memory_overload_at + critical_overload_rate_limit_millis
        ):
            return

        max_desired_memory_bytes = self.max_used_memory_ratio * self.max_memory_bytes
        reserve_memory = self.max_memory_bytes * (1 - self.max_used_memory_ratio) * self.reserve_memory_ratio
        critical_overload_bytes = max_desired_memory_bytes + reserve_memory

        if mem_current_bytes is None:
            raise ValueError('mem_current_bytes is None')

        is_critical_overload = mem_current_bytes > critical_overload_bytes

        if is_critical_overload:
            used_percentage = round((mem_current_bytes / self.max_memory_bytes) * 100)

            logger.warning(
                f'Memory is critically overloaded. Using {to_mb(mem_current_bytes)} MB of '
                f'{to_mb(self.max_memory_bytes)} MB ({used_percentage}%). Consider increasing available memory.'
            )

    def _snapshot_event_loop(self: Snapshotter) -> None:
        """Creates a snapshot of current event loop delay."""
        now = datetime.now(tz=timezone.utc)

        snapshot = EventLoopSnapshot(created_at=now, is_overloaded=False, exceeded=timedelta(seconds=0))

        previous_snapshot = self.event_loop_snapshots[-1] if self.event_loop_snapshots else None

        if previous_snapshot:
            created_at = previous_snapshot.created_at
            delta = now - created_at - self.event_loop_snapshot_interval

            if delta.total_seconds() > self.max_blocked.total_seconds():
                snapshot.is_overloaded = True

            snapshot.exceeded = max(delta - self.max_blocked, timedelta(seconds=0))

        self._prune_snapshots(self.event_loop_snapshots, now)
        self.event_loop_snapshots.append(snapshot)

    def _snapshot_cpu(self: Snapshotter, system_info: SystemInfo) -> None:
        """Creates a snapshot of current CPU usage using the Apify platform `systemInfo` event.

        Args:
            system_info: System info
        """
        cpu_current_usage = system_info.cpu_current_usage
        is_cpu_overloaded = system_info.is_cpu_overloaded
        created_at = system_info.created_at if system_info.created_at else datetime.now(tz=timezone.utc)

        if is_cpu_overloaded is None:
            raise ValueError('is_cpu_overloaded is None')

        if cpu_current_usage is None:
            raise ValueError('cpu_current_usage is None')

        snapshot = CpuSnapshot(
            created_at=created_at,
            is_overloaded=is_cpu_overloaded,
            used_ratio=round(cpu_current_usage / 100),
        )

        self._prune_snapshots(self.cpu_snapshots, created_at)

        self.cpu_snapshots.append(snapshot)

    def _snapshot_client(self: Snapshotter) -> None:
        """Creates a snapshot of the current API state by checking for rate limit errors.

        Only errors produced by a 2nd retry of the API call are considered for snapshotting since
        earlier errors may just be caused by a random spike in the number of requests and do not
        necessarily signify API overloading.
        """
        now = datetime.now(tz=timezone.utc)

        # TODO: The TypeScript implementation uses MemoryStorageClient here for getting rate limit errors, in Python
        # implementation we do not support this yet.

        all_error_counts: list = []

        current_err_count = all_error_counts[self.client_rate_limit_error_retry_count] if all_error_counts else 0

        snapshot = ClientSnapshot(
            created_at=now,
            is_overloaded=False,
            rate_limit_error_count=current_err_count,
        )

        previous_snapshot = self.client_snapshots[-1] if self.client_snapshots else None

        if previous_snapshot:
            rate_limit_error_count = previous_snapshot.rate_limit_error_count
            delta = current_err_count - rate_limit_error_count
            if delta > self.max_client_errors:
                snapshot.is_overloaded = True

        self._prune_snapshots(self.client_snapshots, now)

        self.client_snapshots.append(snapshot)

    def _prune_snapshots(self: Snapshotter, snapshots: Sequence[Snapshot], now: datetime) -> None:
        """Removes snapshots that are older than the `self.snapshot_history_millis` option from the array.

        Args:
            snapshots: List of snapshots.
            now: The current date and time.
        """
        old_count = 0

        for snapshot in snapshots:
            created_at = snapshot.created_at
            if now - created_at > self.snapshot_history:
                old_count += 1
            else:
                break

        snapshots = snapshots[:old_count]

    async def _get_memory_info(self: Snapshotter) -> MemoryInfo:
        """Helper method for easier mocking."""
        return await get_memory_info()
