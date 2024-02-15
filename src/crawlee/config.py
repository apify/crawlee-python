from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass
class Config:
    """Configuration data class for Crawlee.

    Attributes:
        system_info_interval: Defines the interval of emitting the `SystemInfo` event.

        max_used_cpu_ratio: Sets the ratio, defining the maximum CPU usage. When the CPU usage is higher than
            the provided ratio, the CPU is considered overloaded.

        max_used_memory_ratio: Sets the ratio, defining the maximum memory usage. When the memory usage is higher
            than the provided ratio, the memory is considered overloaded.

        max_event_loop_delay: Sets the maximum delay of the event loop. When the delay is higher than the provided
            value, the event loop is considered overloaded.

        max_client_errors: Sets the maximum number of client errors (HTTP 429). When the number of client errors
            is higher than the provided number, the client is considered overloaded.

        memory_mbytes: Sets the amount of system memory in megabytes to be used by the `AutoscaledPool`.
            By default, the maximum memory is set to one quarter of total system memory.
    """

    system_info_interval: timedelta = timedelta(seconds=60)
    max_used_cpu_ratio: float = 0.95
    max_used_memory_ratio: float = 0.7
    max_event_loop_delay: timedelta = timedelta(milliseconds=50)
    max_client_errors: int = 1
    memory_mbytes: int = 0
