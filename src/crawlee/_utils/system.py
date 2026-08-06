from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging import WARNING, getLogger
from typing import TYPE_CHECKING, Annotated

import psutil
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator

from crawlee._utils import cgroup
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce

logger = getLogger(__name__)
logger_once = LoggerOnce(logger)

# Reading a memory metric of a process that is denied or gone raises either a `psutil.Error` or a bare `OSError` -
# psutil re-raises `FileNotFoundError` as is when a `/proc` entry is missing for a process that is still alive.
_METRIC_ERRORS = (psutil.Error, OSError)

_CPU_SAMPLE_INTERVAL_SECS = 0.1
"""How long a CPU measurement waits when it has no earlier reading to compare against."""


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

    In a container with a memory limit this is the limit, not the memory of the host machine, so that a budget derived
    from it cannot exceed what the container is allowed to use.
    """

    system_wide_used_size: Annotated[
        ByteSize,
        PlainValidator(ByteSize.validate),
        PlainSerializer(lambda size: size.bytes),
        Field(alias='systemWideUsedSize'),
    ]
    """Total memory used within the scope `total_size` covers, including memory used by non-crawlee processes.

    In a container with a memory limit this is the working set charged against that limit, the same kind of figure
    `docker stats` and `kubectl top` report. Elsewhere it is the memory used across the host machine.
    """


@dataclass(frozen=True)
class _CpuReading:
    """A cumulative CPU time reading of a cgroup, with the moment it was taken."""

    used_seconds: float
    """The CPU time the cgroup has consumed since it was created, in seconds."""

    taken_at: float
    """The value of the monotonic clock at the moment the reading was taken."""


class _CgroupCpu:
    """Process-wide latch holding the previous cgroup CPU reading.

    A cgroup exposes consumed CPU time as a counter that only grows, so a usage ratio takes two readings taken at
    different moments. `psutil.cpu_percent` keeps equivalent state of its own internally. Only the event manager
    samples the CPU, one call at a time, so the latch needs no synchronization of its own.
    """

    previous: _CpuReading | None = None


def _get_cpu_set_size() -> float | None:
    """Get the number of cores the cgroup of this process is pinned to, or `None` when that is not a restriction."""
    host_cores = psutil.cpu_count()
    cpu_set_cores = cgroup.get_cpu_set_size()

    if host_cores is None or cpu_set_cores is None or cpu_set_cores >= host_cores:
        return None

    return float(cpu_set_cores)


def _get_allowed_cpu_cores() -> float | None:
    """Get the number of CPU cores this process may use, or `None` when nothing restricts it.

    A bandwidth quota and a CPU set restrict it independently, and the tighter one wins. Both are read from the cgroup
    of this process, so they cover the same scope as the CPU time they get paired with.
    """
    limits = [limit for limit in (cgroup.get_cpu_quota(), _get_cpu_set_size()) if limit is not None]

    return min(limits) if limits else None


def _log_cpu_usage_unavailable() -> None:
    """Report that a CPU limit was found but cannot be measured against, so the whole machine is measured instead."""
    logger_once.log(
        'Found a cgroup CPU limit but no usage metric to pair it with, so the load of the whole machine is measured '
        'instead - the autoscaler may scale past the CPU this container is allowed to use.',
        key='cgroup_cpu_usage_unavailable',
        level=WARNING,
    )


def _take_cpu_reading() -> _CpuReading | None:
    """Read the cumulative CPU time of the cgroup of this process, with the moment the reading was taken."""
    used_seconds = cgroup.get_cpu_usage()

    return _CpuReading(used_seconds=used_seconds, taken_at=time.monotonic()) if used_seconds is not None else None


def _get_cgroup_cpu_used_ratio() -> float | None:
    """Get the CPU usage of the cgroup of this process relative to the cores it is allowed to use.

    Returns:
        The ratio, or `None` when nothing restricts the CPU of this process or the cgroup metrics cannot be read.
    """
    allowed_cores = _get_allowed_cpu_cores()
    if allowed_cores is None:
        return None

    previous = _CgroupCpu.previous
    if previous is None:
        # A counter that only grows takes two readings to turn into a rate. Waiting for the second one costs the same
        # as the interval `psutil` spends on its own first call, and keeps every sample in the scope of the cgroup.
        previous = _take_cpu_reading()
        if previous is None:
            _log_cpu_usage_unavailable()
            return None

        # Kept even if the reading below fails, because a counter that only grows can be compared against any earlier
        # reading. Without this, every later sample would wait out the interval again.
        _CgroupCpu.previous = previous
        time.sleep(_CPU_SAMPLE_INTERVAL_SECS)

    reading = _take_cpu_reading()
    if reading is None:
        _log_cpu_usage_unavailable()
        return None

    _CgroupCpu.previous = reading

    elapsed_seconds = reading.taken_at - previous.taken_at
    if elapsed_seconds <= 0:
        return None

    used_ratio = (reading.used_seconds - previous.used_seconds) / (elapsed_seconds * allowed_cores)

    # Moving the process to another cgroup restarts the counter, which makes the difference negative.
    return min(max(used_ratio, 0.0), 1.0)


def get_cpu_info() -> CpuInfo:
    """Retrieve the current CPU usage.

    When a container restricts the CPU of this process, the usage is measured against the cores that container may
    use, read from its cgroup. Without such a restriction the process competes for the whole machine, so the
    system-wide utilization reported by `psutil.cpu_percent()` is used instead.
    """
    logger.debug('Calling get_cpu_info()...')

    used_ratio = _get_cgroup_cpu_used_ratio()

    if used_ratio is None:
        used_ratio = psutil.cpu_percent(interval=_CPU_SAMPLE_INTERVAL_SECS) / 100

    return CpuInfo(used_ratio=used_ratio)


def get_memory_info() -> MemoryInfo:
    """Retrieve the current memory usage of the process and its children.

    It utilizes the `psutil` library. The reported `current_size` is best-effort - processes that cannot be inspected
    are left out of the sum, and PSS may be substituted by RSS for some or all of the processes. The system-wide
    figures come from the cgroup of this process whenever it limits how much memory the process may use.
    """
    logger.debug('Calling get_memory_info()...')
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
    """Narrow the system-wide memory metrics down to the cgroup this process runs in.

    `/proc/meminfo`, which `psutil` reads, is not namespaced, so in a memory-limited container it reports the memory
    of the host instead of the limit the process actually has. Left uncorrected, the autoscaler derives its budget
    from the host total and can keep scaling until the container is killed instead of throttling.

    Args:
        host_total_bytes: Total memory of the host.
        host_used_bytes: Memory used across the host.

    Returns:
        The total and the used memory to report.
    """
    memory_limit = cgroup.get_memory_limit()

    # A cgroup without a memory limit reports one at least as large as the memory of the host. cgroup v1 spells it as
    # a sentinel close to the largest signed 64-bit integer, and runtimes differ on its exact value, so the host total
    # is the reliable thing to compare against.
    if memory_limit is None or memory_limit.limit >= host_total_bytes:
        return host_total_bytes, host_used_bytes

    return memory_limit.limit, memory_limit.working_set
