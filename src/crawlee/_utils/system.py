from __future__ import annotations

import os
from contextlib import suppress
from datetime import datetime, timezone
from logging import getLogger
from typing import Annotated, Any

import psutil
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator

from crawlee._utils.byte_size import ByteSize

logger = getLogger(__name__)


class CpuInfo(BaseModel):
    """Information about the CPU usage."""

    model_config = ConfigDict(populate_by_name=True)

    used_ratio: Annotated[float, Field(alias='usedRatio')]
    """The ratio of CPU currently in use, represented as a float between 0 and 1."""

    created_at: datetime = Field(
        alias='createdAt',
        default_factory=lambda: datetime.now(timezone.utc),
    )
    """The time at which the measurement was taken."""


class MemoryUsageInfo(BaseModel):
    """Information about the memory usage."""

    model_config = ConfigDict(populate_by_name=True)

    current_size: Annotated[
        ByteSize,
        PlainValidator(ByteSize.validate),
        PlainSerializer(lambda size: size.bytes),
        Field(alias='currentSize'),
    ]
    """Memory usage of the current Python process and its children."""

    created_at: datetime = Field(
        alias='createdAt',
        default_factory=lambda: datetime.now(timezone.utc),
    )
    """The time at which the measurement was taken."""


class MemoryInfo(MemoryUsageInfo):
    """Information about system memory."""

    model_config = ConfigDict(populate_by_name=True)

    total_size: Annotated[
        ByteSize, PlainValidator(ByteSize.validate), PlainSerializer(lambda size: size.bytes), Field(alias='totalSize')
    ]
    """Total memory available in the system."""


def get_cpu_info() -> CpuInfo:
    """Retrieve the current CPU usage.

    It utilizes the `psutil` library. Function `psutil.cpu_percent()` returns a float representing the current
    system-wide CPU utilization as a percentage.
    """
    logger.debug('Calling get_cpu_info()...')
    cpu_percent = psutil.cpu_percent(interval=0.1)
    return CpuInfo(used_ratio=cpu_percent / 100)


def get_memory_info() -> MemoryInfo:
    """Retrieve the current memory usage of the process and its children.

    It utilizes the `psutil` library.
    """
    logger.debug('Calling get_memory_info()...')
    current_process = psutil.Process(os.getpid())

    # Retrieve the Resident Set Size (RSS) of the current process. RSS is the portion of memory
    # occupied by a process that is held in RAM.
    # Use RSS as a conservative estimate that can be overestimating due to including shared memory as opposed to USS.
    current_size_bytes = int(current_process.memory_full_info().rss)

    # Sum memory usage by all children processes, try to exclude shared memory from the sum if allowed by OS.
    for child in current_process.children(recursive=True):
        # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
        # its memory usage.
        with suppress(psutil.NoSuchProcess):
            # In the case of children try to estimate memory usage from USS to avoid overestimation due to shared memory
            current_size_bytes += _get_used_memory(child.memory_full_info())

    total_size_bytes = psutil.virtual_memory().total

    return MemoryInfo(
        total_size=ByteSize(total_size_bytes),
        current_size=ByteSize(current_size_bytes),
    )


def _get_used_memory(memory_full_info: Any) -> int:
    """Get the most suitable available used memory metric.

    `Unique Set Size (USS)` is the memory which is unique to a process and which would be freed if the process was
        terminated right now. It should be available on Linux, macOS, Windows.
    `Resident Set Size (RSS)` is the non-swapped physical memory a process has used; it includes shared memory. It
        should be available everywhere.
    """
    try:
        # Overwhelming majority use-case (Linux, macOS, Windows)
        return int(memory_full_info.uss)
    except AttributeError:
        # Very rare use-case
        # Memory usage estimation can overestimate memory usage due to shared memory inclusion.
        return int(memory_full_info.rss)
