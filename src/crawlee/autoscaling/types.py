from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Union


@dataclass
class LoadRatioInfo:
    """Represents the load ratio of a resource."""

    limit_ratio: float
    actual_ratio: float

    @property
    def is_overloaded(self) -> bool:
        """Returns whether the resource is overloaded."""
        return self.actual_ratio > self.limit_ratio


@dataclass
class SystemInfo:
    """Represents the current status of the system."""

    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    cpu_info: LoadRatioInfo | None = None
    mem_info: LoadRatioInfo | None = None
    event_loop_info: LoadRatioInfo | None = None
    client_info: LoadRatioInfo | None = None
    mem_current_bytes: int | None = None  # Platform only property
    cpu_current_usage: int | None = None  # Platform only property

    @property
    def is_system_idle(self) -> bool:
        """Indicates whether the system is currently idle or overloaded."""
        if self.mem_info is None or self.event_loop_info is None or self.cpu_info is None or self.client_info is None:
            raise ValueError('SystemInfo is missing some load ratio info.')

        return (
            not self.cpu_info.is_overloaded
            and not self.mem_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.client_info.is_overloaded
        )

    def __str__(self) -> str:
        """Get a string representation of the system info."""
        stats = {
            'cpu': self.cpu_info.actual_ratio if self.cpu_info else '-',
            'mem': self.mem_info.actual_ratio if self.mem_info else '-',
            'event_lopp': self.event_loop_info.actual_ratio if self.event_loop_info else '-',
            'client_info': self.client_info.actual_ratio if self.client_info else '-',
        }
        return ';'.join(f'{name} = {ratio}' for name, ratio in stats.items())


@dataclass
class FinalStatistics:
    """Represents final statistics."""

    requests_finished: int
    requests_failed: int
    retry_histogram: list[int]
    request_avg_failed_duration: timedelta
    request_avg_finished_duration: timedelta
    requests_finished_per_minute: float
    requests_failed_per_minute: float
    request_total_duration: timedelta
    requests_total: int
    crawler_runtime: timedelta


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage."""

    is_overloaded: bool
    used_bytes: int
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


@dataclass
class CpuSnapshot:
    """A snapshot of CPU usage."""

    used_ratio: float
    max_used_ratio: float
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_overloaded(self: CpuSnapshot) -> bool:
        """Returns whether the CPU is considered as overloaded."""
        return self.used_ratio > self.max_used_ratio


@dataclass
class EventLoopSnapshot:
    """Snapshot of the state of the event loop.

    Args:
        max_delay: The maximum delay that is considered acceptable.
        delay: The actual delay.
        created_at: The time at which the snapshot was created.
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
    """A snapshot of client usage."""

    created_at: datetime
    is_overloaded: bool
    rate_limit_error_count: int


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]
