from __future__ import annotations

import os
from contextlib import suppress
from datetime import datetime, timezone
from logging import getLogger
from typing import Annotated

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
    current_size_bytes = int(current_process.memory_info().rss)

    for child in current_process.children(recursive=True):
        # Ignore any NoSuchProcess exception that might occur if a child process ends before we retrieve
        # its memory usage.
        with suppress(psutil.NoSuchProcess):
            current_size_bytes += int(child.memory_info().rss)

    total_size_bytes = psutil.virtual_memory().total

    return MemoryInfo(
        total_size=ByteSize(total_size_bytes),
        current_size=ByteSize(current_size_bytes),
    )
