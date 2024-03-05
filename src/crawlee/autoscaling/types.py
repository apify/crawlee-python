from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Union


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
        mem_info: The memory load ratio.
        event_loop_info: The event loop load ratio.
        client_info: The client load ratio.
        created_at: The time at which the measurement was taken.
    """

    cpu_info: LoadRatioInfo
    mem_info: LoadRatioInfo
    event_loop_info: LoadRatioInfo
    client_info: LoadRatioInfo
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_system_idle(self) -> bool:
        """Indicates whether the system is currently idle or overloaded."""
        return (
            not self.cpu_info.is_overloaded
            and not self.mem_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.client_info.is_overloaded
        )

    def __str__(self) -> str:
        """Get a string representation of the system info."""
        stats = {
            'cpu': self.cpu_info.actual_ratio,
            'mem': self.mem_info.actual_ratio,
            'event_lopp': self.event_loop_info.actual_ratio,
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
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_overloaded(self: CpuSnapshot) -> bool:
        """Returns whether the CPU is considered as overloaded."""
        return self.used_ratio > self.max_used_ratio


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage.

    Args:
        total_bytes: Total memory available in the system.
        current_bytes: Memory usage of the current Python process and its children.
        max_memory_bytes: The maximum memory that can be used by `AutoscaledPool`.
        max_used_memory_ratio: The maximum acceptable ratio of `current_bytes` to `max_memory_bytes`.
        created_at: The time at which the measurement was taken.
    """

    total_bytes: int
    current_bytes: int
    max_memory_bytes: int
    max_used_memory_ratio: float
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_overloaded(self: MemorySnapshot) -> bool:
        """Returns whether the memory is considered as overloaded."""
        return (self.current_bytes / self.max_memory_bytes) > self.max_used_memory_ratio


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
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def max_delay_exceeded(self: EventLoopSnapshot) -> timedelta:
        """Returns the amount of time by which the delay exceeds the maximum delay."""
        return max(self.delay - self.max_delay, timedelta(seconds=0))

    @property
    def is_overloaded(self: EventLoopSnapshot) -> bool:
        """Returns whether the event loop is considered as overloaded."""
        return self.delay > self.max_delay


@dataclass
class ClientSnapshot:
    """Snapshot of the state of the client.

    Args:
        num_of_errors: The number of errors (HTTP 429) that occurred.
        max_num_of_errors: The maximum number of errors that is considered acceptable.
        created_at: The time at which the measurement was taken.
    """

    num_of_errors: int
    max_num_of_errors: int
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_overloaded(self: ClientSnapshot) -> bool:
        """Returns whether the client is considered as overloaded."""
        return self.num_of_errors > self.max_num_of_errors


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]
