from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from crawlee._utils.byte_size import ByteSize


class ConcurrencySettings:
    """Concurrency settings for AutoscaledPool."""

    def __init__(
        self,
        min_concurrency: int = 1,
        max_concurrency: int = 200,
        max_tasks_per_minute: float = float('inf'),
        desired_concurrency: int | None = None,
    ) -> None:
        """Initialize the ConcurrencySettings.

        Args:
            min_concurrency: The minimum number of tasks running in parallel. If you set this value too high
                with respect to the available system memory and CPU, your code might run extremely slow or crash.

            max_concurrency: The maximum number of tasks running in parallel.

            max_tasks_per_minute: The maximum number of tasks per minute the pool can run. By default, this is set
                to infinity, but you can pass any positive, non-zero number.

            desired_concurrency: The desired number of tasks that should be running parallel on the start of the pool,
                if there is a large enough supply of them. By default, it is `min_concurrency`.
        """
        if desired_concurrency is not None and desired_concurrency < 1:
            raise ValueError('desired_concurrency must be 1 or larger')

        if min_concurrency < 1:
            raise ValueError('min_concurrency must be 1 or larger')

        if max_concurrency < min_concurrency:
            raise ValueError('max_concurrency cannot be less than min_concurrency')

        if max_tasks_per_minute <= 0:
            raise ValueError('max_tasks_per_minute must be positive')

        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.desired_concurrency = desired_concurrency if desired_concurrency is not None else min_concurrency
        self.max_tasks_per_minute = max_tasks_per_minute


@dataclass
class LoadRatioInfo:
    """Represents the load ratio of a resource.

    Args:
        limit_ratio: The maximum ratio of overloaded and non-overloaded samples. If the actual ratio exceeds this
            value, the resource is considered as overloaded.

        actual_ratio: The actual ratio of overloaded and non-overloaded samples.
    """

    limit_ratio: float
    actual_ratio: float

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the resource is overloaded."""
        return self.actual_ratio > self.limit_ratio


@dataclass
class SystemInfo:
    """Represents the current status of the system.

    Args:
        cpu_info: The CPU load ratio.
        memory_info: The memory load ratio.
        event_loop_info: The event loop load ratio.
        client_info: The client load ratio.
        created_at: The time at which the measurement was taken.
    """

    cpu_info: LoadRatioInfo
    memory_info: LoadRatioInfo
    event_loop_info: LoadRatioInfo
    client_info: LoadRatioInfo
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_system_idle(self) -> bool:
        """Indicates whether the system is currently idle or overloaded."""
        return (
            not self.cpu_info.is_overloaded
            and not self.memory_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.client_info.is_overloaded
        )

    def __str__(self) -> str:
        """Get a string representation of the system info."""
        stats = {
            'cpu': self.cpu_info.actual_ratio,
            'mem': self.memory_info.actual_ratio,
            'event_loop': self.event_loop_info.actual_ratio,
            'client_info': self.client_info.actual_ratio,
        }
        return '; '.join(f'{name} = {ratio}' for name, ratio in stats.items())


@dataclass
class CpuSnapshot:
    """A snapshot of CPU usage.

    Args:
        used_ratio: The ratio of CPU currently in use.
        max_used_ratio: The maximum ratio of CPU that is considered acceptable.
        created_at: The time at which the measurement was taken.
    """

    used_ratio: float
    max_used_ratio: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the CPU is considered as overloaded."""
        return self.used_ratio > self.max_used_ratio


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage.

    Args:
        total_size: Total memory available in the system.
        current_size: Memory usage of the current Python process and its children.
        max_memory_size: The maximum memory that can be used by `AutoscaledPool`.
        max_used_memory_ratio: The maximum acceptable ratio of `current_size` to `max_memory_size`.
        created_at: The time at which the measurement was taken.
    """

    total_size: ByteSize
    current_size: ByteSize
    max_memory_size: ByteSize
    max_used_memory_ratio: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the memory is considered as overloaded."""
        return (self.current_size / self.max_memory_size) > self.max_used_memory_ratio


@dataclass
class EventLoopSnapshot:
    """Snapshot of the state of the event loop.

    Args:
        delay: The current delay of the event loop.
        max_delay: The maximum delay that is considered acceptable.
        created_at: The time at which the measurement was taken.
    """

    delay: timedelta
    max_delay: timedelta
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def max_delay_exceeded(self) -> timedelta:
        """Returns the amount of time by which the delay exceeds the maximum delay."""
        return max(self.delay - self.max_delay, timedelta(seconds=0))

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the event loop is considered as overloaded."""
        return self.delay > self.max_delay


@dataclass
class ClientSnapshot:
    """Snapshot of the state of the client.

    Args:
        error_count: The number of errors (HTTP 429) that occurred.
        max_error_count: The maximum number of errors that is considered acceptable.
        created_at: The time at which the measurement was taken.
    """

    error_count: int
    max_error_count: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the client is considered as overloaded."""
        return self.error_count > self.max_error_count


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]
