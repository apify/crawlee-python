from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger

import psutil

logger = getLogger(__name__)


@dataclass
class CpuInfo:
    """Describes CPU usage of the process."""

    current_usage_ratio: float  # Current CPU usage ratio
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


@dataclass
class MemoryInfo:
    """Describes memory usage of the process."""

    total_bytes: int  # Total memory available in the system
    available_bytes: int  # Amount of free memory in the system
    used_bytes: int  # Amount of memory currently in use
    current_process_bytes: int  # Memory usage of the main (current) Python process
    child_processes_bytes: int  # Combined memory usage of all child processes spawned by the current Python process
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


async def get_cpu_info() -> CpuInfo:
    """Retrieves the current CPU usage.

    It utilizes the `psutil` library. Function `psutil.cpu_percent()` returns a float representing the current
    system-wide CPU utilization as a percentage.
    """
    logger.debug('Calling get_cpu_info()...')
    cpu_percent = await asyncio.to_thread(psutil.cpu_percent, interval=0.1)
    current_usage_ratio = cpu_percent / 100
    return CpuInfo(current_usage_ratio=current_usage_ratio)


# TODO: upravit tak, aby se sbiralo info pouze co je potreba
def get_memory_info() -> MemoryInfo:
    """Retrieves the current memory usage of the process and its children.

    It utilizes the `psutil` library.
    """
    logger.debug('Calling get_memory_info()...')
    current_process = psutil.Process(os.getpid())

    # Retrieve the Resident Set Size (RSS) of the current process. RSS is the portion of memory
    # occupied by a process that is held in RAM.
    current_process_bytes = int(current_process.memory_info().rss)

    child_processes_bytes = 0
    for child in current_process.children(recursive=True):
        # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
        # its memory usage.
        with suppress(psutil.NoSuchProcess):
            child_processes_bytes += int(child.memory_info().rss)

    virtual_memory = psutil.virtual_memory()

    return MemoryInfo(
        total_bytes=virtual_memory.total,
        available_bytes=virtual_memory.available,
        used_bytes=virtual_memory.used,
        current_process_bytes=current_process_bytes,
        child_processes_bytes=child_processes_bytes,
    )
