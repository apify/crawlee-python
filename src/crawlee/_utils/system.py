from __future__ import annotations

import os
import sys
from contextlib import suppress
from datetime import datetime, timezone
from logging import WARNING, getLogger
from typing import TYPE_CHECKING, Annotated

import psutil
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce

logger = getLogger(__name__)
logger_once = LoggerOnce(logger)

if sys.platform == 'linux':
    """Get the most suitable available used memory metric.

    `Proportional Set Size (PSS)`, is the amount of own memory and memory shared with other processes, accounted in a
    way that the shared amount is divided evenly between the processes that share it. Available on Linux. Suitable for
    avoiding overestimation by counting the same shared memory used by children processes multiple times.

    `Resident Set Size (RSS)` is the non-swapped physical memory a process has used; it includes shared memory. It
    should be available everywhere.
    """

    def _get_used_memory(process: psutil.Process) -> int:
        # A restricted environment may deny `/proc/<pid>/smaps` or not expose it at all - psutil then aliases
        # `memory_full_info` to `memory_info`, which has no `pss`. An unreadable `smaps` parses to a PSS of zero.
        try:
            pss = getattr(process.memory_full_info(), 'pss', 0)
        except psutil.AccessDenied:
            pss = 0

        if pss:
            return int(pss)

        # RSS counts shared memory in full for every process that maps it, so a sharing process tree is overestimated.
        logger_once.log(
            'Unable to read the PSS memory metric, falling back to RSS - shared memory may be counted repeatedly.',
            key='pss_unavailable',
            level=WARNING,
        )
        return int(process.memory_info().rss)
else:

    def _get_used_memory(process: psutil.Process) -> int:
        return int(process.memory_info().rss)


class CpuInfo(BaseModel):
    """Information about the CPU usage."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    used_ratio: Annotated[float, Field(alias='usedRatio')]
    """The ratio of CPU currently in use, represented as a float between 0 and 1."""

    # Workaround for Pydantic and type checkers when using Annotated with default_factory
    if TYPE_CHECKING:
        created_at: datetime = datetime.now(timezone.utc)
        """The time at which the measurement was taken."""
    else:
        created_at: Annotated[
            datetime,
            Field(
                alias='createdAt',
                default_factory=lambda: datetime.now(timezone.utc),
            ),
        ]
        """The time at which the measurement was taken."""


class MemoryUsageInfo(BaseModel):
    """Information about the memory usage."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    current_size: Annotated[
        ByteSize,
        PlainValidator(ByteSize.validate),
        PlainSerializer(lambda size: size.bytes),
        Field(alias='currentSize'),
    ]
    """Memory usage of the current Python process and its children."""

    # Workaround for Pydantic and type checkers when using Annotated with default_factory
    if TYPE_CHECKING:
        created_at: datetime = datetime.now(timezone.utc)
        """The time at which the measurement was taken."""
    else:
        created_at: Annotated[
            datetime,
            Field(
                alias='createdAt',
                default_factory=lambda: datetime.now(timezone.utc),
            ),
        ]
        """The time at which the measurement was taken."""


class MemoryInfo(MemoryUsageInfo):
    """Information about system memory."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    total_size: Annotated[
        ByteSize, PlainValidator(ByteSize.validate), PlainSerializer(lambda size: size.bytes), Field(alias='totalSize')
    ]
    """Total memory available in the system."""

    system_wide_used_size: Annotated[
        ByteSize,
        PlainValidator(ByteSize.validate),
        PlainSerializer(lambda size: size.bytes),
        Field(alias='systemWideUsedSize'),
    ]
    """Total memory used by all processes system-wide (including non-crawlee processes)."""


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

    It utilizes the `psutil` library. The reported `current_size` is best-effort - processes that cannot be inspected
    are left out of the sum and PSS may be substituted by RSS.
    """
    logger.debug('Calling get_memory_info()...')
    current_process = psutil.Process(os.getpid())

    # Retrieve estimated memory usage of the current process.
    current_size_bytes = _get_used_memory(current_process)

    # Sum memory usage by all children processes, try to exclude shared memory from the sum if allowed by OS.
    children: list[psutil.Process] = []
    try:
        children = current_process.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        # A missing child list hides the whole subprocess tree from the estimate, so do not degrade silently.
        logger_once.log(
            'Unable to list child processes, their memory usage is excluded from the estimate.',
            key='children_unavailable',
            level=WARNING,
        )

    for child in children:
        # Skip a child that exits mid-loop (`NoSuchProcess`) or that we cannot inspect (`AccessDenied`); either way
        # it drops out of the sum.
        with suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            current_size_bytes += _get_used_memory(child)

    vm = psutil.virtual_memory()

    return MemoryInfo(
        total_size=ByteSize(vm.total),
        current_size=ByteSize(current_size_bytes),
        system_wide_used_size=ByteSize(vm.total - vm.available),
    )
