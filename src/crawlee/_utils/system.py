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

    # Retrieve estimated memory usage of the current process.
    current_size_bytes = int(_get_used_memory(current_process.memory_full_info()))

    # Sum memory usage by all children processes, try to exclude shared memory from the sum if allowed by OS.
    for child in current_process.children(recursive=True):
        # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
        # its memory usage.
        with suppress(psutil.NoSuchProcess):
            current_size_bytes += _get_used_memory(child.memory_full_info())

    total_size_bytes = psutil.virtual_memory().total

    return MemoryInfo(
        total_size=ByteSize(total_size_bytes),
        current_size=ByteSize(current_size_bytes),
    )


def _get_used_memory(memory_full_info: Any) -> int:
    """Get the most suitable available used memory metric.

    `Proportional Set Size (PSS)`, is the amount of own memory and memory shared with other processes, accounted in a
    way that the shared amount is divided evenly between the processes that share it. Available on Linux. Suitable for
    avoiding overestimation by counting the same shared memory used by children processes multiple times.

    `Resident Set Size (RSS)` is the non-swapped physical memory a process has used; it includes shared memory. It
    should be available everywhere.
    """
    try:
        # Linux
        return int(memory_full_info.pss)
    except AttributeError:
        return int(memory_full_info.rss)
