from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from logging import WARNING, getLogger
from pathlib import Path, PurePosixPath

from crawlee._utils.log import LoggerOnce

logger = getLogger(__name__)
logger_once = LoggerOnce(logger)

_PROC_SELF_CGROUP = Path('/proc/self/cgroup')
"""Lists the cgroup this process belongs to, one line per mounted hierarchy."""

_PROC_SELF_MOUNTINFO = Path('/proc/self/mountinfo')
"""Lists the mounted filesystems. Read to locate the hierarchies instead of assuming `/sys/fs/cgroup`."""

_MICROSECONDS_PER_SECOND = 1_000_000
_NANOSECONDS_PER_SECOND = 1_000_000_000

_V1_CONTROLLER_NAMES = frozenset({'memory', 'cpu', 'cpuacct', 'cpuset'})
"""The controllers worth recording. A cgroup v1 mount lists them among options that name no controller at all."""


@dataclass(frozen=True)
class _Hierarchy:
    """A mounted cgroup hierarchy, together with the cgroup this process belongs to in it."""

    point: Path
    """The directory the hierarchy is mounted at."""

    root: str
    """The subtree of the hierarchy the mount exposes, spelled the same way as `own_path`."""

    own_path: str
    """The cgroup this process belongs to, as `/proc/self/cgroup` spells it."""


@dataclass(frozen=True)
class _Controller:
    """A located controller: the interface it provides and the directories to read its control files from."""

    is_v2: bool
    """Whether the controller provides the cgroup v2 interface, which spells its control files differently."""

    dirs: tuple[Path, ...]
    """The cgroup of this process first, then each of its ancestors up to the top of the hierarchy.

    A limit set on an ancestor caps everything below it. Under Kubernetes the container, the pod and the QoS class
    each get a level of their own, and the limit that matters may sit on any of them.
    """


@dataclass(frozen=True)
class _Controllers:
    """The controllers that carry resource metrics, as located for this process."""

    memory: _Controller | None
    """The controller carrying the memory limit and the memory charged against it."""

    cpu_quota: _Controller | None
    """The controller carrying the CPU bandwidth quota."""

    cpu_usage: _Controller | None
    """The controller carrying the consumed CPU time. Under cgroup v1 that is `cpuacct`, a controller of its own."""

    cpu_set: _Controller | None
    """The controller carrying the set of cores the cgroup may run on."""


@dataclass(frozen=True)
class MemoryLimit:
    """A cgroup memory limit, together with the memory usage charged against it."""

    limit: int
    """The tightest limit applying to this process, in bytes."""

    working_set: int
    """The memory charged against the limit, in bytes, excluding reclaimable file cache.

    Several cgroups can hold a limit, and the one closest to running out is not always the one holding the tightest
    limit. This is the highest utilization among them, expressed against `limit` so that the two stay comparable.
    """


def get_memory_limit() -> MemoryLimit | None:
    """Get the tightest memory limit applying to this process, with the usage measured against it.

    A usage read at one level next to a limit read at another compares two different scopes, and the ratio between
    them then says nothing about how close an out-of-memory kill is. Both numbers are therefore brought to the same
    scale before being returned.

    Returns:
        The limit and the working set, or `None` when no cgroup limit applies to this process or the control files
        cannot be read.
    """
    controller = _get_controllers().memory
    if controller is None:
        return None

    # Only the hard limit counts. A cgroup can sit above `memory.high` indefinitely, because it throttles reclaim
    # rather than triggering an out-of-memory kill.
    file_name = 'memory.max' if controller.is_v2 else 'memory.limit_in_bytes'

    # Exceeding the tightest limit gets the process killed whichever level holds it, so that one is the ceiling a
    # memory budget has to fit under. Which level runs out first is a separate question, because a limit further up
    # covers the sibling cgroups too. The utilization is therefore taken from the level closest to its own limit,
    # wherever along the chain that sits.
    ceiling: int | None = None
    used_ratios = []

    for directory in controller.dirs:
        limit = _read_int(directory / file_name)
        if limit is None or limit <= 0:
            continue

        # A level counts towards the ceiling on the strength of its limit alone. Its usage is a separate reading that
        # can be missing, and dropping the limit along with it would raise the ceiling above what the kernel enforces.
        ceiling = limit if ceiling is None else min(ceiling, limit)

        working_set = _read_working_set(controller, directory)
        if working_set is not None:
            used_ratios.append(working_set / limit)

    if ceiling is None:
        return None

    if not used_ratios:
        logger_once.log(
            'Found a cgroup memory limit but no usage metric to pair it with, so the limit is ignored - the '
            'autoscaler may scale past the limit of this container.',
            key='cgroup_memory_usage_unavailable',
            level=WARNING,
        )
        return None

    # A cgroup can sit above its limit while the kernel reclaims, which is not a usage the caller can act on.
    used_ratio = min(max(used_ratios), 1.0)

    return MemoryLimit(limit=ceiling, working_set=round(used_ratio * ceiling))


def get_cpu_quota() -> float | None:
    """Get the number of CPU cores the bandwidth quota allows this process to use.

    Returns:
        The number of cores, which can be fractional, or `None` when no quota applies to this process or the control
        files cannot be read.
    """
    controller = _get_controllers().cpu_quota
    if controller is None:
        return None

    read_quota = _read_cpu_quota_v2 if controller.is_v2 else _read_cpu_quota_v1

    # Levels that hold no readable quota drop out, so an unlimited ancestor does not hide a quota set below it.
    quotas = [quota for directory in controller.dirs if (quota := read_quota(directory)) is not None]

    return min(quotas) if quotas else None


def get_cpu_set_size() -> int | None:
    """Get the number of CPU cores the cgroup of this process is allowed to run on.

    Reading the cgroup rather than the affinity of the process keeps this in the same scope as the CPU time it gets
    paired with. A `taskset` narrows one process without narrowing the cgroup around it.

    Returns:
        The number of cores, or `None` when no CPU set applies to this process or the control file cannot be read.
    """
    controller = _get_controllers().cpu_set
    if controller is None:
        return None

    # The effective set already accounts for the ancestors, because a cgroup is never given cores its parent lacks.
    file_name = 'cpuset.cpus.effective' if controller.is_v2 else 'cpuset.cpus'

    try:
        cpu_list = (controller.dirs[0] / file_name).read_text().strip()
    except OSError:
        return None

    # Under cgroup v1 an empty set means the cgroup inherits the cores of its parent.
    return _count_cpu_list(cpu_list) if cpu_list else None


def get_cpu_usage() -> float | None:
    """Get the CPU time the cgroup of this process has consumed since the cgroup was created, in seconds.

    Unlike the memory metrics, this reads the own cgroup rather than the level the quota sits on. A quota set on an
    ancestor alone is not how container runtimes spell a CPU limit, so the two levels coincide in practice.

    Returns:
        The cumulative CPU time, or `None` when it cannot be read.
    """
    controller = _get_controllers().cpu_usage
    if controller is None:
        return None

    own_dir = controller.dirs[0]

    if controller.is_v2:
        microseconds = _read_stat_value(own_dir / 'cpu.stat', 'usage_usec')
        return microseconds / _MICROSECONDS_PER_SECOND if microseconds is not None else None

    nanoseconds = _read_int(own_dir / 'cpuacct.usage')
    return nanoseconds / _NANOSECONDS_PER_SECOND if nanoseconds is not None else None


def _read_working_set(controller: _Controller, directory: Path) -> int | None:
    """Read the memory charged to one cgroup, in bytes, excluding reclaimable file cache.

    The raw usage a cgroup reports counts the page cache, which the kernel drops on demand and which therefore does
    not predict an out-of-memory kill. Subtracting the inactive file cache gives the working set, the same figure
    `docker stats`, `kubectl top` and cAdvisor report.
    """
    current = _read_int(directory / ('memory.current' if controller.is_v2 else 'memory.usage_in_bytes'))
    if current is None:
        return None

    # Falling back to the raw usage would turn this into "usage including reclaimable cache", which is the confusion
    # the subtraction exists to prevent.
    inactive_file_key = 'inactive_file' if controller.is_v2 else 'total_inactive_file'
    inactive_file = _read_stat_value(directory / 'memory.stat', inactive_file_key)
    if inactive_file is None:
        return None

    return max(current - inactive_file, 0)


@lru_cache(maxsize=1)
def _get_controllers() -> _Controllers:
    """Locate the control files that carry the resource metrics of this process.

    Locating them walks `/proc`, which costs orders of magnitude more than reading a single control file, and the
    result stays valid for the lifetime of the process. The control files themselves are read again on every sample.
    """
    try:
        unified, v1 = _read_hierarchies()
    except OSError:
        # Either this is not Linux, or `/proc` is not mounted. No cgroup metrics exist to be read in both cases.
        return _Controllers(memory=None, cpu_quota=None, cpu_usage=None, cpu_set=None)

    return _Controllers(
        memory=_locate_controller(unified, v1, 'memory', v2_probe='memory.current', v1_probe='memory.usage_in_bytes'),
        cpu_quota=_locate_controller(unified, v1, 'cpu', v2_probe='cpu.max', v1_probe='cpu.cfs_quota_us'),
        # Under cgroup v1 the quota and the accounting belong to two controllers that can be mounted separately.
        cpu_usage=_locate_controller(unified, v1, 'cpuacct', v2_probe='cpu.stat', v1_probe='cpuacct.usage'),
        cpu_set=_locate_controller(unified, v1, 'cpuset', v2_probe='cpuset.cpus.effective', v1_probe='cpuset.cpus'),
    )


def _locate_controller(
    unified: _Hierarchy | None,
    v1: dict[str, _Hierarchy],
    v1_name: str,
    *,
    v2_probe: str,
    v1_probe: str,
) -> _Controller | None:
    """Locate the directories holding the files of one controller, preferring the cgroup v2 unified hierarchy.

    A system can mount both interfaces at once with only some of the controllers enabled on the unified hierarchy, so
    a candidate counts only where the probe file it is supposed to provide exists.
    """
    for hierarchy, probe_file, is_v2 in ((unified, v2_probe, True), (v1.get(v1_name), v1_probe, False)):
        if hierarchy is None:
            continue

        dirs = _trim_to_controller(_candidate_dirs(hierarchy), probe_file)
        if dirs:
            return _Controller(is_v2=is_v2, dirs=dirs)

    return None


def _trim_to_controller(dirs: tuple[Path, ...], probe_file: str) -> tuple[Path, ...]:
    """Drop the levels below the closest one that carries the controller."""
    # A cgroup gets a controller's files only once its parent enables that controller for its children, so the levels
    # closest to the process can be missing them while the levels above still declare the limit that applies.
    for index, directory in enumerate(dirs):
        if (directory / probe_file).exists():
            return dirs[index:]

    return ()


def _candidate_dirs(hierarchy: _Hierarchy) -> tuple[Path, ...]:
    """List the directories a controller's files can be read from, the cgroup of this process first."""
    path = PurePosixPath(hierarchy.own_path)

    # A mount can expose just a subtree of a hierarchy, and then the paths in `/proc/self/cgroup` carry the mount root
    # as a prefix that has to come off. Container runtimes instead give the container a cgroup namespace of its own.
    if hierarchy.root != '/':
        try:
            path = PurePosixPath('/') / path.relative_to(hierarchy.root)
        except ValueError:
            # The mount does not cover the cgroup of this process, so only the top of the mount is worth reading.
            path = PurePosixPath('/')

    parts = path.parts[1:] if path.is_absolute() else path.parts
    own_dir = hierarchy.point.joinpath(*parts)

    # Walking up stops at the mount point, because nothing above it belongs to the hierarchy.
    return (own_dir, *own_dir.parents[: len(parts)])


def _read_hierarchies() -> tuple[_Hierarchy | None, dict[str, _Hierarchy]]:
    """Locate the mounted cgroup hierarchies and the cgroup this process belongs to in each of them.

    Returns:
        The cgroup v2 unified hierarchy, and the cgroup v1 hierarchies keyed by the controller each one carries.

    Raises:
        OSError: If `/proc/self/mountinfo` or `/proc/self/cgroup` cannot be read.
    """
    unified_path, controller_paths = _read_own_paths()

    unified: _Hierarchy | None = None
    controllers: dict[str, _Hierarchy] = {}

    for line in _PROC_SELF_MOUNTINFO.read_text().splitlines():
        # A variable number of optional fields sits between the mount point and the ` - ` separator, so the line has
        # to be split on the separator first.
        before, separator, after = line.partition(' - ')
        if not separator:
            continue

        try:
            _mount_id, _parent_id, _device, mount_root, mount_point, *_ = before.split(' ')
            filesystem, _source, super_options, *_ = after.split(' ')
        except ValueError:
            continue

        if filesystem == 'cgroup2' and unified_path is not None:
            # The same hierarchy can be bind-mounted a second time, for instance by an agent that watches the host
            # from inside a container. Mounts are listed in the order they were made, so the first one is ours.
            unified = unified or _Hierarchy(point=Path(mount_point), root=mount_root, own_path=unified_path)
        elif filesystem == 'cgroup':
            # A cgroup v1 mount names the controllers it carries among its super options, e.g. `rw,cpu,cpuacct`.
            for option in super_options.split(','):
                own_path = controller_paths.get(option)
                if option in _V1_CONTROLLER_NAMES and own_path is not None:
                    hierarchy = _Hierarchy(point=Path(mount_point), root=mount_root, own_path=own_path)
                    controllers.setdefault(option, hierarchy)

    return unified, controllers


def _read_own_paths() -> tuple[str | None, dict[str, str]]:
    """Read the cgroup this process belongs to in the unified hierarchy and in each cgroup v1 one.

    Raises:
        OSError: If `/proc/self/cgroup` cannot be read.
    """
    unified: str | None = None
    controllers: dict[str, str] = {}

    for line in _PROC_SELF_CGROUP.read_text().splitlines():
        try:
            _hierarchy_id, controller_list, cgroup_path = line.split(':', 2)
        except ValueError:
            continue

        # The unified hierarchy is the entry with no controllers listed, spelled `0::<path>`.
        if not controller_list:
            unified = cgroup_path
            continue

        for controller in controller_list.split(','):
            # A cgroup v1 hierarchy mounted without a controller carries a name instead, e.g. `name=systemd`.
            controllers[controller.removeprefix('name=')] = cgroup_path

    return unified, controllers


def _count_cpu_list(cpu_list: str) -> int | None:
    """Count the CPUs a control file lists as a mix of ranges and single numbers, e.g. `0-3,8`."""
    count = 0

    try:
        for part in cpu_list.split(','):
            first, separator, last = part.partition('-')
            count += int(last) - int(first) + 1 if separator else 1
    except ValueError:
        return None

    return count


def _read_cpu_quota_v2(directory: Path) -> float | None:
    """Read the number of cores allowed by the quota and the period a cgroup v2 `cpu.max` file holds."""
    try:
        quota, period = (directory / 'cpu.max').read_text().split()
        # An unlimited cgroup spells the quota as `max`, which is not an integer.
        return int(quota) / int(period)
    except (OSError, ValueError, ZeroDivisionError):
        return None


def _read_cpu_quota_v1(directory: Path) -> float | None:
    """Read the number of cores the cgroup v1 quota and period files allow."""
    quota = _read_int(directory / 'cpu.cfs_quota_us')
    period = _read_int(directory / 'cpu.cfs_period_us')

    # An unlimited cgroup sets the quota to a negative value.
    if quota is None or period is None or quota <= 0 or period <= 0:
        return None

    return quota / period


def _read_int(path: Path) -> int | None:
    """Read a control file holding a single integer, or `None` if it holds anything else."""
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        # cgroup v2 spells an absent limit as `max`, which is not an integer.
        return None


def _read_stat_value(path: Path, key: str) -> int | None:
    """Read one entry of a control file holding a flat table of `<key> <value>` lines."""
    try:
        with path.open() as file:
            for line in file:
                entry_key, _separator, value = line.partition(' ')
                if entry_key == key:
                    return int(value)
    except (OSError, ValueError):
        return None

    return None
