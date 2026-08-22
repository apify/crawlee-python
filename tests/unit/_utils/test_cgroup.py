from __future__ import annotations

import logging
from itertools import count
from types import SimpleNamespace
from typing import TYPE_CHECKING

import psutil
import pytest

from crawlee._utils import cgroup, system
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

V2_MOUNTINFO = '25 30 0:22 / {root} rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate'
"""A single unified hierarchy exposed from its top, which is what a container runtime sets up."""

V1_MOUNTINFO = (
    '29 25 0:25 / {root}/systemd rw,nosuid shared:9 - cgroup cgroup rw,name=systemd\n'
    '30 25 0:26 / {root}/memory rw,nosuid shared:14 - cgroup cgroup rw,memory\n'
    '31 25 0:27 / {root}/cpu,cpuacct rw,nosuid shared:15 - cgroup cgroup rw,cpu,cpuacct\n'
    '32 25 0:28 / {root}/cpuset rw,nosuid shared:16 - cgroup cgroup rw,cpuset'
)
"""One hierarchy per controller, with the CPU accounting sharing a mount with the CPU bandwidth controller."""

V2_SELF_CGROUP = '0::{path}\n'
"""The unified hierarchy is the entry with no controllers listed."""

V1_SELF_CGROUP = '4:cpuset:{path}\n3:cpu,cpuacct:{path}\n2:memory:{path}\n1:name=systemd:{path}\n'
"""One entry per cgroup v1 hierarchy, including the named one that carries no controller."""

HOST_TOTAL_BYTES = 8 * 1024**3
HOST_AVAILABLE_BYTES = 3 * 1024**3
HOST_CORES = 8


@pytest.fixture(autouse=True)
def _isolated_module_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Reset the process-wide state of both modules, so that nothing leaks between tests."""
    monkeypatch.setattr(system, 'logger_once', LoggerOnce(system.logger))
    monkeypatch.setattr(cgroup, 'logger_once', LoggerOnce(cgroup.logger))
    monkeypatch.setattr(system._CgroupCpu, 'previous', None)
    cgroup._get_controllers.cache_clear()
    yield
    cgroup._get_controllers.cache_clear()


@pytest.fixture
def fake_cgroup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Path]:
    """Return a builder that lays out a fake cgroup filesystem, points the module at it and returns its root."""

    def build(*, mountinfo: str, self_cgroup: str, files: dict[str, str]) -> Path:
        root = tmp_path / 'cgroup'
        root.mkdir(parents=True, exist_ok=True)

        for relative_path, content in files.items():
            path = root / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

        mountinfo_path = tmp_path / 'mountinfo'
        mountinfo_path.write_text(mountinfo.format(root=root))
        self_cgroup_path = tmp_path / 'self_cgroup'
        self_cgroup_path.write_text(self_cgroup)

        monkeypatch.setattr(cgroup, '_PROC_SELF_MOUNTINFO', mountinfo_path)
        monkeypatch.setattr(cgroup, '_PROC_SELF_CGROUP', self_cgroup_path)

        return root

    return build


@pytest.fixture
def _no_cgroup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the module at `/proc` files that do not exist, as on a system without cgroups."""
    monkeypatch.setattr(cgroup, '_PROC_SELF_MOUNTINFO', tmp_path / 'missing')
    monkeypatch.setattr(cgroup, '_PROC_SELF_CGROUP', tmp_path / 'missing')


@pytest.fixture
def _fixed_host_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the host memory `psutil` reports, so the expected values do not move with the machine running the tests."""
    monkeypatch.setattr(
        psutil,
        'virtual_memory',
        lambda: SimpleNamespace(total=HOST_TOTAL_BYTES, available=HOST_AVAILABLE_BYTES),
    )


@pytest.fixture
def _fixed_host_cores(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the core count `psutil` reports, so a limit stays a limit whatever machine runs the tests."""
    monkeypatch.setattr(psutil, 'cpu_count', lambda: HOST_CORES)


@pytest.fixture
def _one_second_per_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    """Advance the clock the module reads by one second per reading, so a usage rate comes out predictable."""
    clock = count(start=100.0, step=1.0)
    monkeypatch.setattr(system, 'time', SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda _seconds: None))


def test_read_hierarchies_v2(fake_cgroup: Callable[..., Path]) -> None:
    """Finds the unified hierarchy and the cgroup this process belongs to in it."""
    root = fake_cgroup(mountinfo=V2_MOUNTINFO, self_cgroup=V2_SELF_CGROUP.format(path='/init.scope'), files={})

    unified, controllers = cgroup._read_hierarchies()

    assert unified is not None
    assert unified.point == root
    assert unified.root == '/'
    assert unified.own_path == '/init.scope'
    assert controllers == {}


def test_read_hierarchies_v1(fake_cgroup: Callable[..., Path]) -> None:
    """Finds every controller of a cgroup v1 mount that carries more than one."""
    root = fake_cgroup(mountinfo=V1_MOUNTINFO, self_cgroup=V1_SELF_CGROUP.format(path='/docker/abc'), files={})

    unified, controllers = cgroup._read_hierarchies()

    assert unified is None
    assert controllers['memory'].point == root / 'memory'
    assert controllers['memory'].own_path == '/docker/abc'
    assert controllers['cpuset'].point == root / 'cpuset'
    # Otherwise the CPU metrics get split across versions.
    assert controllers['cpu'].point == root / 'cpu,cpuacct'
    assert controllers['cpuacct'].point == root / 'cpu,cpuacct'


def test_read_hierarchies_bad_lines(fake_cgroup: Callable[..., Path]) -> None:
    """Skips the lines it cannot parse, which a table of every filesystem on the machine is full of."""
    mountinfo = f'not a mount line\n24 30 0:21 / /sys rw - sysfs sysfs rw\n{V2_MOUNTINFO}\n25 30 0:22 /'
    root = fake_cgroup(mountinfo=mountinfo, self_cgroup=V2_SELF_CGROUP.format(path='/'), files={})

    unified, _controllers = cgroup._read_hierarchies()

    assert unified is not None
    assert unified.point == root


@pytest.mark.parametrize(
    ('escaped', 'expected'),
    [
        pytest.param('/plain/path', '/plain/path', id='nothing to decode'),
        pytest.param('/mnt\\040point', '/mnt point', id='space'),
        pytest.param('/tab\\011here', '/tab\there', id='tab'),
        pytest.param('/back\\134slash', '/back\\slash', id='backslash'),
        pytest.param('/literal\\134040', '/literal\\040', id='escaped backslash in front of an octal sequence'),
    ],
)
def test_unescape(escaped: str, expected: str) -> None:
    """Decodes the octal sequences a path field of the mount table escapes special characters as."""
    assert cgroup._unescape(escaped) == expected


def test_read_hierarchies_escaped_paths(fake_cgroup: Callable[..., Path]) -> None:
    """Decodes both path fields, which `/proc/self/cgroup` spells unescaped and so cannot be compared against."""
    mountinfo = '25 30 0:22 /docker\\040abc {root}/mnt\\040point rw shared:4 - cgroup2 cgroup2 rw'
    root = fake_cgroup(mountinfo=mountinfo, self_cgroup=V2_SELF_CGROUP.format(path='/docker abc'), files={})

    unified, _controllers = cgroup._read_hierarchies()

    assert unified is not None
    assert unified.point == root / 'mnt point'
    assert unified.root == '/docker abc'


@pytest.mark.parametrize(
    ('self_cgroup', 'expected_unified', 'expected_controllers'),
    [
        pytest.param('0::/init.scope\n', '/init.scope', {}, id='unified only'),
        pytest.param(
            '2:memory:/docker/abc\n1:name=systemd:/docker/abc\n',
            None,
            {'memory': '/docker/abc', 'systemd': '/docker/abc'},
            id='v1 only, with a named hierarchy',
        ),
        pytest.param(
            '2:memory:/system.slice\n0::/user.slice\n',
            '/user.slice',
            {'memory': '/system.slice'},
            id='both interfaces at once',
        ),
        pytest.param('nonsense\n0::/\n', '/', {}, id='unparsable line skipped'),
    ],
)
def test_read_own_paths(
    fake_cgroup: Callable[..., Path],
    self_cgroup: str,
    expected_unified: str | None,
    expected_controllers: dict[str, str],
) -> None:
    """Reads the cgroup this process belongs to in each hierarchy."""
    fake_cgroup(mountinfo=V2_MOUNTINFO, self_cgroup=self_cgroup, files={})

    unified, controllers = cgroup._read_own_paths()

    assert unified == expected_unified
    assert controllers == expected_controllers


@pytest.mark.parametrize(
    ('mount_root', 'cgroup_path', 'expected'),
    [
        pytest.param('/', '/', [''], id='own cgroup at the top of the mount'),
        pytest.param(
            '/',
            '/kubepods/pod/container',
            ['kubepods/pod/container', 'kubepods/pod', 'kubepods', ''],
            id='nested',
        ),
        pytest.param('/docker/abc', '/docker/abc/nested', ['nested', ''], id='mount root stripped'),
        pytest.param('/docker/abc', '/system.slice', [''], id='mount does not cover the own cgroup'),
    ],
)
def test_candidate_dirs(
    tmp_path: Path,
    mount_root: str,
    cgroup_path: str,
    expected: list[str],
) -> None:
    """Walks from the cgroup of this process up to the top of the mount."""
    hierarchy = cgroup._Hierarchy(point=tmp_path, root=mount_root, own_path=cgroup_path)

    dirs = cgroup._candidate_dirs(hierarchy)

    assert list(dirs) == [tmp_path / relative if relative else tmp_path for relative in expected]


@pytest.mark.parametrize(
    ('mountinfo', 'self_cgroup', 'files', 'expected'),
    [
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'memory.max': '536870912\n', 'memory.current': '1000\n', 'memory.stat': 'inactive_file 400\n'},
            536870912,
            id='v2 limit',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'memory.max': 'max\n', 'memory.current': '1000\n', 'memory.stat': 'inactive_file 400\n'},
            None,
            id='v2 unlimited',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/pod/container'),
            {
                'pod/container/memory.max': 'max\n',
                'pod/container/memory.current': '1000\n',
                'pod/container/memory.stat': 'inactive_file 400\n',
                'pod/memory.max': '268435456\n',
                'pod/memory.current': '9000\n',
                'pod/memory.stat': 'inactive_file 1000\n',
            },
            268435456,
            id='limit inherited from an ancestor',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            {
                'memory/memory.limit_in_bytes': '536870912\n',
                'memory/memory.usage_in_bytes': '1000\n',
                'memory/memory.stat': 'total_inactive_file 400\n',
            },
            536870912,
            id='v1 limit',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            # Only a caller that knows the memory of the host can tell the sentinel apart from a real limit.
            {
                'memory/memory.limit_in_bytes': '9223372036854771712\n',
                'memory/memory.usage_in_bytes': '1000\n',
                'memory/memory.stat': 'total_inactive_file 400\n',
            },
            9223372036854771712,
            id='v1 unlimited sentinel',
        ),
    ],
)
def test_get_memory_limit(
    fake_cgroup: Callable[..., Path],
    mountinfo: str,
    self_cgroup: str,
    files: dict[str, str],
    expected: int | None,
) -> None:
    """Reads the memory limit under both cgroup interfaces."""
    fake_cgroup(mountinfo=mountinfo, self_cgroup=self_cgroup, files=files)

    memory_limit = cgroup.get_memory_limit()

    assert (memory_limit.limit if memory_limit is not None else None) == expected


def test_get_memory_limit_tightest_level(fake_cgroup: Callable[..., Path]) -> None:
    """Reports the tightest limit of the chain, so a budget cannot exceed what the kernel enforces."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/kubepods/pod/container'),
        files={
            # The node-level cgroup is generous and busy, because every pod on the node is charged against it.
            'kubepods/memory.max': '8000\n',
            'kubepods/memory.current': '6000\n',
            'kubepods/memory.stat': 'inactive_file 0\n',
            # The container this process runs in is limited far more tightly, and barely uses its share.
            'kubepods/pod/container/memory.max': '1000\n',
            'kubepods/pod/container/memory.current': '100\n',
            'kubepods/pod/container/memory.stat': 'inactive_file 0\n',
        },
    )

    memory_limit = cgroup.get_memory_limit()

    assert memory_limit is not None
    assert memory_limit.limit == 1000
    assert memory_limit.working_set == 750


def test_get_memory_limit_worst_level(fake_cgroup: Callable[..., Path]) -> None:
    """Reports the utilization of the level closest to running out, which is not always the tightest one."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/qos/pod'),
        files={
            # The pod is nearly full of its own generous limit.
            'qos/pod/memory.max': '1000\n',
            'qos/pod/memory.current': '960\n',
            'qos/pod/memory.stat': 'inactive_file 10\n',
            # The QoS class above is tighter, but half of what it holds belongs to the sibling pods.
            'qos/memory.max': '500\n',
            'qos/memory.current': '260\n',
            'qos/memory.stat': 'inactive_file 10\n',
        },
    )

    memory_limit = cgroup.get_memory_limit()

    assert memory_limit is not None
    assert memory_limit.limit == 500
    assert memory_limit.working_set == 475


def test_get_memory_limit_partial_level(fake_cgroup: Callable[..., Path]) -> None:
    """Counts a level towards the ceiling even when the usage next to it cannot be read."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/kubepods/pod/container'),
        files={
            'kubepods/memory.max': '8000\n',
            'kubepods/memory.current': '6000\n',
            'kubepods/memory.stat': 'inactive_file 0\n',
            # The tightest limit, with no page cache metric to derive a working set from.
            'kubepods/pod/container/memory.max': '1000\n',
            'kubepods/pod/container/memory.current': '100\n',
            'kubepods/pod/container/memory.stat': 'anon 100\n',
        },
    )

    memory_limit = cgroup.get_memory_limit()

    assert memory_limit is not None
    assert memory_limit.limit == 1000
    assert memory_limit.working_set == 750


@pytest.mark.parametrize(
    ('mountinfo', 'self_cgroup', 'files'),
    [
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {
                'memory.max': '536870912\n',
                'memory.current': '1000\n',
                'memory.stat': 'anon 600\ninactive_file 400\nfile 400\n',
            },
            id='v2',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            {
                'memory/memory.limit_in_bytes': '536870912\n',
                'memory/memory.usage_in_bytes': '1000\n',
                'memory/memory.stat': 'rss 600\ntotal_inactive_file 400\n',
            },
            id='v1',
        ),
    ],
)
def test_get_memory_limit_working_set(
    fake_cgroup: Callable[..., Path],
    mountinfo: str,
    self_cgroup: str,
    files: dict[str, str],
) -> None:
    """Subtracts the page cache the kernel drops on demand, which would not predict a kill."""
    fake_cgroup(mountinfo=mountinfo, self_cgroup=self_cgroup, files=files)

    memory_limit = cgroup.get_memory_limit()

    assert memory_limit is not None
    assert memory_limit.working_set == 600


def test_get_memory_limit_no_working_set(
    fake_cgroup: Callable[..., Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Drops a limit that no usage can be paired with, and warns instead of pairing it with the raw usage."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'memory.max': '536870912\n', 'memory.current': '1000\n', 'memory.stat': 'anon 600\n'},
    )

    with caplog.at_level(logging.WARNING, logger=cgroup.logger.name):
        assert cgroup.get_memory_limit() is None

    assert [record for record in caplog.records if 'cgroup memory limit' in record.getMessage()]


def test_get_memory_limit_missing_files(fake_cgroup: Callable[..., Path]) -> None:
    """Reads the closest level that carries the controller, which the own cgroup need not."""
    root = fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/pod/container'),
        files={
            'pod/memory.max': '268435456\n',
            'pod/memory.current': '1000\n',
            'pod/memory.stat': 'inactive_file 400\n',
        },
    )
    # The cgroup of the process exists, it just carries no memory files of its own.
    (root / 'pod' / 'container').mkdir()

    memory_limit = cgroup.get_memory_limit()

    assert memory_limit is not None
    assert memory_limit.limit == 268435456
    assert memory_limit.working_set == 600


@pytest.mark.parametrize(
    ('mountinfo', 'self_cgroup', 'files', 'expected'),
    [
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpu.max': '200000 100000\n', 'cpu.stat': 'usage_usec 0\n'},
            2.0,
            id='v2 quota',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpu.max': '50000 100000\n', 'cpu.stat': 'usage_usec 0\n'},
            0.5,
            id='v2 fractional quota',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpu.max': 'max 100000\n', 'cpu.stat': 'usage_usec 0\n'},
            None,
            id='v2 unlimited',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/pod/container'),
            {
                'pod/container/cpu.max': 'max 100000\n',
                'pod/container/cpu.stat': 'usage_usec 0\n',
                'pod/cpu.max': '150000 100000\n',
            },
            1.5,
            id='v2 quota inherited from an ancestor',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            {
                'cpu,cpuacct/cpu.cfs_quota_us': '150000\n',
                'cpu,cpuacct/cpu.cfs_period_us': '100000\n',
                'cpu,cpuacct/cpuacct.usage': '0\n',
            },
            1.5,
            id='v1 quota',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            {
                'cpu,cpuacct/cpu.cfs_quota_us': '-1\n',
                'cpu,cpuacct/cpu.cfs_period_us': '100000\n',
                'cpu,cpuacct/cpuacct.usage': '0\n',
            },
            None,
            id='v1 unlimited',
        ),
    ],
)
def test_get_cpu_quota(
    fake_cgroup: Callable[..., Path],
    mountinfo: str,
    self_cgroup: str,
    files: dict[str, str],
    expected: float | None,
) -> None:
    """Reads the CPU bandwidth quota under both cgroup interfaces."""
    fake_cgroup(mountinfo=mountinfo, self_cgroup=self_cgroup, files=files)

    assert cgroup.get_cpu_quota() == expected


@pytest.mark.parametrize(
    ('mountinfo', 'self_cgroup', 'files', 'expected'),
    [
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpuset.cpus.effective': '0-1\n'},
            2,
            id='v2 range',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpuset.cpus.effective': '0-1,4,6-7\n'},
            5,
            id='v2 ranges mixed with single cores',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpuset.cpus.effective': '\n'},
            None,
            id='inherited from the parent',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            {'cpuset/cpuset.cpus': '0-3\n'},
            4,
            id='v1',
        ),
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            {'cpuset.cpus.effective': '0-1,nonsense\n'},
            None,
            id='every entry has to parse, not just the ranges',
        ),
    ],
)
def test_get_cpu_set_size(
    fake_cgroup: Callable[..., Path],
    mountinfo: str,
    self_cgroup: str,
    files: dict[str, str],
    expected: int | None,
) -> None:
    """Counts the cores of a CPU set spelled as a mix of ranges and single numbers."""
    fake_cgroup(mountinfo=mountinfo, self_cgroup=self_cgroup, files=files)

    assert cgroup.get_cpu_set_size() == expected


@pytest.mark.parametrize(
    ('mountinfo', 'self_cgroup', 'files'),
    [
        pytest.param(
            V2_MOUNTINFO,
            V2_SELF_CGROUP.format(path='/'),
            # cgroup v2 reports the consumed CPU time in microseconds, among other keys.
            {'cpu.stat': 'usage_usec 2500000\nuser_usec 2000000\n'},
            id='v2',
        ),
        pytest.param(
            V1_MOUNTINFO,
            V1_SELF_CGROUP.format(path='/'),
            # cgroup v1 reports it in nanoseconds, in a file of its own.
            {'cpu,cpuacct/cpuacct.usage': '2500000000\n'},
            id='v1',
        ),
    ],
)
def test_get_cpu_usage(
    fake_cgroup: Callable[..., Path],
    mountinfo: str,
    self_cgroup: str,
    files: dict[str, str],
) -> None:
    """Reports the consumed CPU time in seconds under both cgroup interfaces."""
    fake_cgroup(mountinfo=mountinfo, self_cgroup=self_cgroup, files=files)

    assert cgroup.get_cpu_usage() == 2.5


def test_get_controllers_hybrid(fake_cgroup: Callable[..., Path]) -> None:
    """Falls back to cgroup v1 for a controller the unified hierarchy does not carry."""
    fake_cgroup(
        mountinfo=f'{V2_MOUNTINFO}\n{V1_MOUNTINFO}',
        self_cgroup=f'{V2_SELF_CGROUP.format(path="/")}2:memory:/\n',
        files={
            'memory/memory.limit_in_bytes': '536870912\n',
            'memory/memory.usage_in_bytes': '1000\n',
            'memory/memory.stat': 'total_inactive_file 400\n',
        },
    )

    memory = cgroup._get_controllers().memory
    memory_limit = cgroup.get_memory_limit()

    assert memory is not None
    assert memory.is_v2 is False
    assert memory_limit is not None
    assert memory_limit.limit == 536870912
    assert memory_limit.working_set == 600


@pytest.mark.usefixtures('_no_cgroup')
def test_no_cgroups() -> None:
    """Reports nothing on a system that has no cgroups."""
    assert cgroup.get_memory_limit() is None
    assert cgroup.get_cpu_quota() is None
    assert cgroup.get_cpu_set_size() is None
    assert cgroup.get_cpu_usage() is None


# The tests below cover how `system.py` reports the metrics read above, which is the only place they are consumed.


@pytest.mark.usefixtures('_fixed_host_memory')
def test_get_memory_info_limited(fake_cgroup: Callable[..., Path]) -> None:
    """Reports the limit of the container, not the memory of the host the autoscaler would size its budget from."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={
            'memory.max': '536870912\n',
            'memory.current': '150000000\n',
            'memory.stat': 'inactive_file 50000000\n',
        },
    )

    memory_info = get_memory_info()

    assert memory_info.total_size == ByteSize(536870912)
    assert memory_info.system_wide_used_size == ByteSize(100000000)


@pytest.mark.usefixtures('_fixed_host_memory')
def test_get_memory_info_unlimited(fake_cgroup: Callable[..., Path]) -> None:
    """Reports the host when the limit is at or above its memory, which is how an unconstrained cgroup spells it."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={
            'memory.max': f'{HOST_TOTAL_BYTES * 2}\n',
            'memory.current': '150000000\n',
            'memory.stat': 'inactive_file 50000000\n',
        },
    )

    memory_info = get_memory_info()

    assert memory_info.total_size == ByteSize(HOST_TOTAL_BYTES)
    assert memory_info.system_wide_used_size == ByteSize(HOST_TOTAL_BYTES - HOST_AVAILABLE_BYTES)


@pytest.mark.usefixtures('_fixed_host_memory')
def test_get_memory_info_no_working_set(fake_cgroup: Callable[..., Path]) -> None:
    """Falls back to the host as a whole, because a cgroup limit next to host-wide usage compares two scopes."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'memory.max': '536870912\n', 'memory.current': '150000000\n', 'memory.stat': 'anon 1\n'},
    )

    memory_info = get_memory_info()

    assert memory_info.total_size == ByteSize(HOST_TOTAL_BYTES)
    assert memory_info.system_wide_used_size == ByteSize(HOST_TOTAL_BYTES - HOST_AVAILABLE_BYTES)


@pytest.mark.usefixtures('_one_second_per_sample', '_fixed_host_cores')
def test_get_cpu_info_quota(fake_cgroup: Callable[..., Path], monkeypatch: pytest.MonkeyPatch) -> None:
    """Measures the CPU against the bandwidth quota."""
    root = fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'cpu.max': '200000 100000\n', 'cpu.stat': 'usage_usec 0\n'},
    )
    # A host-wide reading would show a fully loaded machine, so it is visible if the fallback is taken by mistake.
    monkeypatch.setattr(psutil, 'cpu_percent', lambda **_: 100.0)

    # A counter that only grows needs two readings, which the first sample waits for instead of falling back.
    assert get_cpu_info().used_ratio == 0.0

    (root / 'cpu.stat').write_text('usage_usec 1000000\n')

    # One core-second over one second of wall time, out of the two cores the quota allows.
    assert get_cpu_info().used_ratio == pytest.approx(0.5)


@pytest.mark.usefixtures('_one_second_per_sample', '_fixed_host_cores')
def test_get_cpu_info_cpu_set(fake_cgroup: Callable[..., Path]) -> None:
    """Measures the CPU against a set that restricts the cores without setting any quota."""
    root = fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'cpuset.cpus.effective': '0-1\n', 'cpu.stat': 'usage_usec 0\n'},
    )
    assert get_cpu_info().used_ratio == 0.0

    (root / 'cpu.stat').write_text('usage_usec 2000000\n')

    # Two core-seconds over one second of wall time saturate the two cores of the set.
    assert get_cpu_info().used_ratio == pytest.approx(1.0)


@pytest.mark.usefixtures('_one_second_per_sample', '_fixed_host_cores')
def test_get_cpu_info_counter_restart(fake_cgroup: Callable[..., Path], monkeypatch: pytest.MonkeyPatch) -> None:
    """Clamps the counter that restarts when the process is moved to another cgroup."""
    root = fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'cpu.max': '200000 100000\n', 'cpu.stat': 'usage_usec 5000000\n'},
    )
    monkeypatch.setattr(psutil, 'cpu_percent', lambda **_: 100.0)

    get_cpu_info()
    (root / 'cpu.stat').write_text('usage_usec 0\n')

    assert get_cpu_info().used_ratio == 0.0


@pytest.mark.usefixtures('_one_second_per_sample', '_fixed_host_cores')
def test_get_cpu_info_failed_reading(fake_cgroup: Callable[..., Path], monkeypatch: pytest.MonkeyPatch) -> None:
    """Keeps the earlier reading when the next one fails, because a counter that only grows stays comparable."""
    root = fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'cpu.max': '200000 100000\n', 'cpu.stat': 'usage_usec 0\n'},
    )
    monkeypatch.setattr(psutil, 'cpu_percent', lambda **_: 100.0)

    assert get_cpu_info().used_ratio == 0.0

    (root / 'cpu.stat').unlink()
    assert get_cpu_info().used_ratio == 1.0

    (root / 'cpu.stat').write_text('usage_usec 1000000\n')

    # Starting over instead would compare the reading against itself and report nothing consumed.
    assert get_cpu_info().used_ratio == pytest.approx(0.5)


@pytest.mark.usefixtures('_fixed_host_cores')
def test_get_cpu_info_no_usage(
    fake_cgroup: Callable[..., Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warns when a CPU limit is found but nothing can be measured against it."""
    fake_cgroup(
        mountinfo=V2_MOUNTINFO,
        self_cgroup=V2_SELF_CGROUP.format(path='/'),
        files={'cpu.max': '200000 100000\n'},
    )
    monkeypatch.setattr(psutil, 'cpu_percent', lambda **_: 42.0)

    with caplog.at_level(logging.WARNING, logger=system.logger.name):
        assert get_cpu_info().used_ratio == pytest.approx(0.42)

    assert [record for record in caplog.records if 'cgroup CPU limit' in record.getMessage()]


@pytest.mark.usefixtures('_no_cgroup')
def test_get_cpu_info_no_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falls back to the load of the whole machine, which is what matters when nothing restricts the CPU."""
    monkeypatch.setattr(psutil, 'cpu_percent', lambda **_: 42.0)

    assert get_cpu_info().used_ratio == pytest.approx(0.42)


@pytest.mark.parametrize(
    ('quota', 'cpu_set_cores', 'host_cores', 'expected'),
    [
        pytest.param(None, None, 8, None, id='nothing restricts the cpu'),
        pytest.param(2.0, None, 8, 2.0, id='bandwidth quota only'),
        pytest.param(None, 2, 8, 2.0, id='cpu set only'),
        pytest.param(None, 8, 8, None, id='a cpu set covering every core is not a restriction'),
        pytest.param(8.0, None, 8, None, id='a quota covering every core is not a restriction'),
        pytest.param(10.0, None, 8, None, id='a quota above the cores of the machine is not a restriction'),
        pytest.param(10.0, 2, 8, 2.0, id='a quota above the machine leaves the cpu set to bind'),
        pytest.param(4.0, 2, 8, 2.0, id='cpu set is tighter than the quota'),
        pytest.param(1.0, 2, 8, 1.0, id='quota is tighter than the cpu set'),
        pytest.param(2.0, None, None, 2.0, id='a quota counts when the cores of the machine are unknown'),
        pytest.param(None, 2, None, 2.0, id='a cpu set counts when the cores of the machine are unknown'),
    ],
)
def test_get_allowed_cpu_cores(
    monkeypatch: pytest.MonkeyPatch,
    quota: float | None,
    cpu_set_cores: int | None,
    host_cores: int | None,
    expected: float | None,
) -> None:
    """Takes the tighter of the bandwidth quota and the CPU set, which restrict the CPU independently."""
    monkeypatch.setattr(cgroup, 'get_cpu_quota', lambda: quota)
    monkeypatch.setattr(cgroup, 'get_cpu_set_size', lambda: cpu_set_cores)
    monkeypatch.setattr(psutil, 'cpu_count', lambda: host_cores)

    assert system._get_allowed_cpu_cores() == expected
