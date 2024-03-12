from __future__ import annotations

import os
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger

import psutil

logger = getLogger(__name__)


@dataclass
class CpuInfo:
    """Information about the CPU usage.

    Args:
        used_ratio: The ratio of CPU currently in use, represented as a float between 0 and 1.
        created_at: The time at which the measurement was taken.
    """

    used_ratio: float
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


@dataclass
class MemoryInfo:
    """Information about the memory usage.

    Args:
        total_bytes: Total memory available in the system.
        current_bytes: current_bytes: Memory usage of the current Python process and its children.
        created_at: The time at which the measurement was taken.
    """

    total_bytes: int
    current_bytes: int
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


def get_cpu_info() -> CpuInfo:
    """Retrieves the current CPU usage.

    It utilizes the `psutil` library. Function `psutil.cpu_percent()` returns a float representing the current
    system-wide CPU utilization as a percentage.
    """
    logger.debug('Calling get_cpu_info()...')
    cpu_percent = psutil.cpu_percent(interval=0.1)
    return CpuInfo(used_ratio=cpu_percent / 100)


def get_memory_info() -> MemoryInfo:
    """Retrieves the current memory usage of the process and its children.

    It utilizes the `psutil` library.
    """
    logger.debug('Calling get_memory_info()...')
    current_process = psutil.Process(os.getpid())

    # Retrieve the Resident Set Size (RSS) of the current process. RSS is the portion of memory
    # occupied by a process that is held in RAM.
    current_bytes = int(current_process.memory_info().rss)

    for child in current_process.children(recursive=True):
        # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
        # its memory usage.
        with suppress(psutil.NoSuchProcess):
            current_bytes += int(child.memory_info().rss)

    virtual_memory = psutil.virtual_memory()

    return MemoryInfo(total_bytes=virtual_memory.total, current_bytes=current_bytes)
