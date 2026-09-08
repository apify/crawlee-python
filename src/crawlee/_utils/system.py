from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timezone
from logging import WARNING, getLogger
from typing import TYPE_CHECKING, Annotated

import cgroups_sensor
import psutil
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce

logger = getLogger(__name__)
logger_once = LoggerOnce(logger)

# Reading a memory metric of a process that is denied or gone raises either a `psutil.Error` or a bare `OSError` -
# psutil re-raises `FileNotFoundError` as is when a `/proc` entry is missing for a process that is still alive.
_METRIC_ERRORS = (psutil.Error, OSError)

_CPU_SAMPLE_INTERVAL_SECS = 0.1
"""How long a CPU fallback measures for. A window shorter than 0.01 seconds is refused by the sensor."""

_cpu_load = cgroups_sensor.CpuLoad()
"""Process-wide CPU sampler, measuring across the gap between calls."""


class _PssAvailability:
    """Process-wide latch for whether the PSS memory metric exists on this system at all.

    Memory is sampled on a short recurring interval, so once psutil is known to not expose PSS there is no point in
    asking for it again for every process on every sample. Only the system-wide verdict is latched; a single process
    refusing to be inspected says nothing about the others.
    """

    is_available = True


if sys.platform == 'linux':

    def _get_used_memory(process: psutil.Process) -> int:
        """Get the most suitable available used memory metric of a single process.

        `Proportional Set Size (PSS)` is the amount of own memory and memory shared with other processes, accounted in
        a way that the shared amount is divided evenly between the processes that share it. Available on Linux.
        Suitable for avoiding overestimation by counting the same shared memory used by children processes multiple
        times.

        `Resident Set Size (RSS)` is the non-swapped physical memory a process has used; it includes shared memory. It
        should be available everywhere, so it is used whenever PSS cannot be read. It counts shared memory in full for
        every process that maps it, so a sharing process tree gets overestimated.

        Raises:
            psutil.Error: If the process refuses inspection or is gone.
            OSError: If a `/proc` entry of the process is missing.
        """
        if _PssAvailability.is_available:
            try:
                # A system that does not expose `smaps` at all makes psutil alias `memory_full_info` to
                # `memory_info`, whose result has no `pss` field.
                memory = process.memory_full_info()
            except psutil.NoSuchProcess:
                # A process that is gone is not refusing inspection, so let the RSS read below fail for it as usual.
                # `ZombieProcess` is a subclass of `NoSuchProcess`, so a zombie lands here too.
                pass
            except _METRIC_ERRORS:
                # A restricted environment may deny `/proc/<pid>/smaps`, which is a property of the single process, so
                # only that one process falls back to RSS. Still worth reporting - when the denial covers the whole
                # process tree, the estimate switches to RSS with nothing else to show it.
                logger_once.log(
                    'Unable to read the PSS memory metric of a process, falling back to RSS for it - shared memory '
                    'may be counted repeatedly.',
                    key='pss_denied',
                    level=WARNING,
                )
            else:
                pss = getattr(memory, 'pss', None)

                if pss is None:
                    _PssAvailability.is_available = False
                    logger_once.log(
                        'Unable to read the PSS memory metric, falling back to RSS - shared memory may be counted '
                        'repeatedly.',
                        key='pss_unavailable',
                        level=WARNING,
                    )
                # A `smaps` file can be empty for some processes, which parses to a PSS of zero. No live process
                # really uses zero memory, so treat it as a missing reading rather than as a measurement.
                elif pss > 0:
                    return int(pss)

                # `memory_full_info` reads the RSS on its way to the PSS, so the fallback does not have to read it
                # again.
                return int(memory.rss)

        return int(process.memory_info().rss)
else:

    def _get_used_memory(process: psutil.Process) -> int:
        """Get the used memory metric of a single process.

        `Resident Set Size (RSS)` is the non-swapped physical memory a process has used; it includes shared memory, so
        a process tree that shares memory gets overestimated. It is the only metric available outside of Linux.

        Raises:
            psutil.Error: If the process refuses inspection or is gone.
            OSError: If the memory metric of the process cannot be read.
        """
        return int(process.memory_info().rss)


def _get_child_used_memory(child: psutil.Process) -> int:
    """Get the used memory of a child process, or zero if the child cannot be measured at all."""
    try:
        return _get_used_memory(child)
    except psutil.NoSuchProcess:
        # A child that exits mid-measurement just drops out of the sum, which is business as usual.
        return 0
    except _METRIC_ERRORS:
        # A child we cannot inspect at all drops out of the sum too, which does hide its memory usage.
        logger_once.log(
            'Unable to read the memory usage of a child process, it is excluded from the estimate.',
            key='child_unmeasurable',
            level=WARNING,
        )
        return 0


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
    """Memory usage of the current Python process and its children.

    This is a best-effort estimate - a process that cannot be inspected is left out of the sum, and the metric used
    may be RSS, which counts memory shared between the processes repeatedly. When only some of the processes expose
    PSS, the sum mixes both metrics, so the memory those processes share with the rest of the tree is counted twice.
    """

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
    """Total memory available to this process.

    Under a container limit this is the limit rather than the memory of the host machine.
    """

    system_wide_used_size: Annotated[
        ByteSize,
        PlainValidator(ByteSize.validate),
        PlainSerializer(lambda size: size.bytes),
        Field(alias='systemWideUsedSize'),
    ]
    """Total memory used within the scope `total_size` covers, including memory used by non-crawlee processes.

    Under a container limit this is the memory charged against that limit, as `docker stats` reports it.
    """


class _ResourceLimits:
    """Process-wide latch keeping the limits report to one line per process, rather than one per sample."""

    is_pending = True
    lock = threading.Lock()


def _log_resource_limits() -> None:
    """Report the limits applying to this process, at most once per process and only where any apply."""
    # The latch is consumed before the reading, so a sensor that raises costs one snapshot rather than every one.
    with _ResourceLimits.lock:
        if not _ResourceLimits.is_pending:
            return
        _ResourceLimits.is_pending = False

    limits = cgroups_sensor.snapshot()
    cores = limits.cpu_limit

    # An unrestricted process is the ordinary case, and a line saying so explains nothing.
    if limits.memory_budget is None and cores is None:
        return

    memory = str(ByteSize(limits.memory_budget.limit)) if limits.memory_budget else 'unrestricted'
    cpu = f'{cores:g} core{"" if cores == 1 else "s"}' if cores is not None else 'unrestricted'
    logger.info(f'Resource limits applying to this process: memory {memory}, CPU {cpu}.')


def get_cpu_info() -> CpuInfo:
    """Retrieve the current CPU usage.

    Under a container limit the load is measured against the cores this process may use. The sampler measures across
    the gap between calls, so the first sample of the process falls back to a short measurement of its own. Without a
    limit the process competes for the whole machine, and `psutil.cpu_percent()` answers instead.
    """
    logger.debug('Calling get_cpu_info()...')

    # Read on every sample rather than latched, because a limit can be resized while the process runs.
    if cgroups_sensor.get_cpu_limit() is None:
        return CpuInfo(used_ratio=psutil.cpu_percent(interval=_CPU_SAMPLE_INTERVAL_SECS) / 100)

    used_ratio = _cpu_load.sample()

    if used_ratio is None:
        used_ratio = cgroups_sensor.get_cpu_used_ratio(_CPU_SAMPLE_INTERVAL_SECS)

    if used_ratio is None:
        used_ratio = psutil.cpu_percent(interval=_CPU_SAMPLE_INTERVAL_SECS) / 100

    return CpuInfo(used_ratio=used_ratio)


def get_memory_info() -> MemoryInfo:
    """Retrieve the current memory usage of the process and its children.

    It utilizes the `psutil` library. The reported `current_size` is best-effort - processes that cannot be inspected
    are left out of the sum, and PSS may be substituted by RSS for some or all of the processes. The system-wide
    figures come from the limit applying to this process whenever one restricts how much memory it may use.
    """
    logger.debug('Calling get_memory_info()...')
    _log_resource_limits()
    current_process = psutil.Process(os.getpid())

    # Retrieve estimated memory usage of the current process. Deliberately not guarded - a process can always read
    # its own RSS, and if it somehow cannot, failing the whole snapshot is safer than reporting a sum that is missing
    # the main process: the autoscaler would read the gap as free memory and keep scaling up.
    current_size_bytes = _get_used_memory(current_process)

    # Sum memory usage by all children processes, try to exclude shared memory from the sum if allowed by OS.
    children: list[psutil.Process] = []
    try:
        children = current_process.children(recursive=True)
    except _METRIC_ERRORS:
        # A missing child list hides the whole subprocess tree from the estimate, so do not degrade silently.
        logger_once.log(
            'Unable to list child processes, their memory usage is excluded from the estimate.',
            key='children_unavailable',
            level=WARNING,
        )

    for child in children:
        current_size_bytes += _get_child_used_memory(child)

    vm = psutil.virtual_memory()
    total_size_bytes, system_wide_used_size_bytes = _get_system_wide_memory(
        host_total_bytes=vm.total,
        host_used_bytes=vm.total - vm.available,
    )

    return MemoryInfo(
        total_size=ByteSize(total_size_bytes),
        current_size=ByteSize(current_size_bytes),
        system_wide_used_size=ByteSize(system_wide_used_size_bytes),
    )


def _get_system_wide_memory(*, host_total_bytes: int, host_used_bytes: int) -> tuple[int, int]:
    """Get the total and the used memory to report, narrowed to the limit applying to this process."""
    budget = cgroups_sensor.get_memory_budget()

    if budget is None:
        return host_total_bytes, host_used_bytes

    # Not clamped to the memory of the machine: a Windows job limits commit, so that would pair a commit charge with
    # a physical ceiling.
    return budget.limit, budget.used
