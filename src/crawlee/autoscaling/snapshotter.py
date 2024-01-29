from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Sequence, Union

if TYPE_CHECKING:
    from datetime import datetime

logger = getLogger(__name__)


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage."""

    created_at: datetime
    is_overloaded: bool
    used_bytes: int | None


@dataclass
class CpuSnapshot:
    """A snapshot of CPU usage."""

    created_at: datetime
    is_overloaded: bool
    used_ratio: float
    ticks: dict | None = None


@dataclass
class EventLoopSnapshot:
    """A snapshot of event loop usage."""

    created_at: datetime
    is_overloaded: bool
    exceeded_millis: float


@dataclass
class ClientSnapshot:
    """A snapshot of client usage."""

    created_at: datetime
    is_overloaded: bool
    rate_limit_error_count: int


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]


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

    Event loop becomes overloaded if it slows down by more than the `max_blocked_millis` option.

    Client becomes overloaded when rate limit errors (429 - Too Many Requests) exceeds the `max_client_errors` option,
    typically received from the request queue, exceed the set limit within the set interval.
    """

    def get_memory_sample(
        self: Snapshotter,
        sample_duration_millis: float | None = None,
    ) -> Sequence[MemorySnapshot]:
        """Returns a sample of the latest memory snapshots.

        Args:
            sample_duration_millis: The size of the sample in milliseconds. Defaults to None.
            If omitted, it returns a full snapshot history.

        Returns:
            A sample of memory snapshots.
        """

    def get_event_loop_sample(
        self: Snapshotter,
        sample_duration_millis: float | None = None,
    ) -> Sequence[EventLoopSnapshot]:
        """Returns a sample of the latest event loop snapshots.

        Args:
            sample_duration_millis: The size of the sample in milliseconds. Defaults to None.
                If omitted, it returns a full snapshot history.

        Returns:
            A sample of event loop snapshots.
        """

    def get_cpu_sample(
        self: Snapshotter,
        sample_duration_millis: float | None = None,
    ) -> Sequence[CpuSnapshot]:
        """Returns a sample of the latest CPU snapshots.

        Args:
            sample_duration_millis: The size of the sample in milliseconds. Defaults to None.
                If omitted, it returns a full snapshot history.

        Returns:
            A sample of CPU snapshots.
        """

    def get_client_sample(
        self: Snapshotter,
        sample_duration_millis: float | None = None,
    ) -> Sequence[ClientSnapshot]:
        """Returns a sample of the latest client snapshots.

        Args:
            sample_duration_millis: The size of the sample in milliseconds. Defaults to None.
                If omitted, it returns a full snapshot history.

        Returns:
            A sample of client snapshots.
        """
