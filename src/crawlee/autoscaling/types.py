from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from datetime import datetime, timedelta


@dataclass
class LoadRatioInfo:
    """Represents the load ratio of a resource."""

    is_overloaded: bool
    limit_ratio: float
    actual_ratio: float


@dataclass
class SystemInfo:
    """Represents the current status of the system."""

    is_system_idle: bool  # Indicates whether the system is currently idle or overloaded
    mem_info: LoadRatioInfo
    event_loop_info: LoadRatioInfo
    cpu_info: LoadRatioInfo
    client_info: LoadRatioInfo
    mem_current_bytes: int | None = None  # Platform only property
    cpu_current_usage: int | None = None  # Platform only property
    is_cpu_overloaded: bool | None = None  # Platform only property
    created_at: datetime | None = None


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
