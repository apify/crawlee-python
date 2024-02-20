from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from datetime import timedelta


@dataclass
class LoadRatioInfo:
    """Represents the load ratio of a resource."""

    limit_ratio: float
    actual_ratio: float

    @property
    def is_overloaded(self: LoadRatioInfo) -> bool:
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
    mem_current_bytes: int | None = None

    @property
    def is_system_idle(self: SystemInfo) -> bool:
        """Indicates whether the system is currently idle or overloaded."""
        if self.mem_info is None or self.event_loop_info is None or self.cpu_info is None or self.client_info is None:
            raise ValueError('SystemInfo is missing some load ratio info.')

        return (
            not self.cpu_info.is_overloaded
            and not self.mem_info.is_overloaded
            and not self.event_loop_info.is_overloaded
            and not self.client_info.is_overloaded
        )


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
    exceeded: timedelta


@dataclass
class ClientSnapshot:
    """A snapshot of client usage."""

    created_at: datetime
    is_overloaded: bool
    rate_limit_error_count: int


Snapshot = Union[MemorySnapshot, CpuSnapshot, EventLoopSnapshot, ClientSnapshot]
