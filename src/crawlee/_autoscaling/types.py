from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from crawlee._utils.byte_size import ByteSize


@dataclass
class LoadRatioInfo:
    """Represent the load ratio of a resource."""

    limit_ratio: float
    """The maximum ratio of overloaded and non-overloaded samples. If the actual ratio exceeds this value,
    the resource is considered as overloaded."""

    actual_ratio: float
    """The actual ratio of overloaded and non-overloaded samples."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the resource is currently overloaded."""
        return self.actual_ratio > self.limit_ratio


@dataclass
class SystemInfo:
    """Represent the current status of the system."""

    cpu_info: LoadRatioInfo
    """The CPU load ratio."""

    memory_info: LoadRatioInfo
    """The memory load ratio."""

    event_loop_info: LoadRatioInfo
    """The event loop load ratio."""

    client_info: LoadRatioInfo
    """The client load ratio."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time at which the system load information was measured."""

    @property
    def is_system_idle(self) -> bool:
        """Indicate whether the system is currently idle or overloaded."""
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
    """A snapshot of CPU usage."""

    used_ratio: float
    """The ratio of CPU currently in use."""

    max_used_ratio: float
    """The maximum ratio of CPU that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the CPU is considered as overloaded."""
        return self.used_ratio > self.max_used_ratio


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage."""

    current_size: ByteSize
    """Memory usage of the current Python process and its children."""

    max_memory_size: ByteSize
    """The maximum memory that can be used by `AutoscaledPool`."""

    max_used_memory_ratio: float
    """The maximum acceptable ratio of `current_size` to `max_memory_size`."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the memory is considered as overloaded."""
        return (self.current_size / self.max_memory_size) > self.max_used_memory_ratio


@dataclass
class EventLoopSnapshot:
    """Snapshot of the state of the event loop."""

    delay: timedelta
    """The current delay of the event loop."""

    max_delay: timedelta
    """The maximum delay that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time at which the system load information was measured."""

    @property
    def max_delay_exceeded(self) -> timedelta:
        """The amount of time by which the delay exceeds the maximum delay."""
        return max(self.delay - self.max_delay, timedelta(seconds=0))

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the event loop is considered as overloaded."""
        return self.delay > self.max_delay


@dataclass
class ClientSnapshot:
    """Snapshot of the state of the client."""

    error_count: int
    """The number of errors (HTTP 429) that occurred."""

    max_error_count: int
    """The maximum number of errors that is considered acceptable."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time at which the system load information was measured."""

    @property
    def is_overloaded(self) -> bool:
        """Indicate whether the client is considered as overloaded."""
        return self.error_count > self.max_error_count


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]
