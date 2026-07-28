from __future__ import annotations

import logging
import sys
from multiprocessing import get_context, synchronize
from multiprocessing.shared_memory import SharedMemory
from types import SimpleNamespace
from typing import TYPE_CHECKING

import psutil
import pytest

from crawlee._utils import system
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from collections.abc import Callable


class FakeProcess:
    """Stand-in for `psutil.Process` that lets a test decide what a child process reports as its memory usage."""

    def __init__(self, pid: int, used_memory: int | Exception) -> None:
        self.pid = pid
        self.used_memory = used_memory

    def memory_full_info(self) -> object:
        if isinstance(self.used_memory, Exception):
            raise self.used_memory
        # Mirrors psutil, whose full result carries the RSS alongside the PSS.
        return SimpleNamespace(rss=self.used_memory, pss=self.used_memory)

    def memory_info(self) -> object:
        if isinstance(self.used_memory, Exception):
            raise self.used_memory
        return SimpleNamespace(rss=self.used_memory)


def raise_access_denied(process: psutil.Process) -> object:
    """Stand in for `psutil.Process.memory_full_info` in an environment that refuses to expose PSS."""
    raise psutil.AccessDenied(pid=process.pid)


@pytest.fixture(autouse=True)
def _isolated_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the process-wide state of the module, so that dedup keys and the PSS latch do not leak between tests."""
    monkeypatch.setattr(system, 'logger_once', LoggerOnce(system.logger))
    monkeypatch.setattr(system._PssAvailability, 'is_available', True)


@pytest.fixture
def measured_current_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the real current process report a fixed memory usage of 100 bytes, whichever metric is used."""
    monkeypatch.setattr(psutil.Process, 'memory_full_info', lambda _process: SimpleNamespace(pss=100))
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=100))


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert ByteSize(0) < memory_info.total_size < ByteSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


@pytest.mark.skipif(sys.platform != 'linux', reason='PSS is read only on Linux, elsewhere RSS is used directly')
@pytest.mark.parametrize(
    ('memory_full_info', 'expected_size', 'expected_warning'),
    [
        pytest.param(raise_access_denied, 2048, False, id='access denied'),
        pytest.param(lambda _process: SimpleNamespace(rss=1024, pss=0), 1024, False, id='pss reported as zero'),
        pytest.param(lambda _process: SimpleNamespace(rss=1024), 1024, True, id='pss field missing'),
    ],
)
def test_get_used_memory_falls_back_to_rss_when_pss_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    memory_full_info: Callable[[psutil.Process], object],
    expected_size: int,
    *,
    expected_warning: bool,
) -> None:
    """An unreadable PSS metric falls back to the RSS of the same process instead of raising or reporting zero."""
    monkeypatch.setattr(psutil.Process, 'memory_full_info', memory_full_info)
    # A value distinct from the RSS of the full result shows that the metric is re-read only when it has to be -
    # a full result that was read successfully already carries the RSS.
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=2048))

    with caplog.at_level(logging.WARNING, logger=system.logger.name):
        assert system._get_used_memory(psutil.Process()) == expected_size

    # Only a system that has no PSS at all is worth reporting - a single process denying it says nothing about the
    # others, and an empty `smaps` parses to zero for a process that is on its way out.
    warnings = [record.getMessage() for record in caplog.records if 'PSS' in record.getMessage()]
    assert bool(warnings) == expected_warning
    assert system._PssAvailability.is_available != expected_warning


@pytest.mark.skipif(sys.platform != 'linux', reason='PSS is read only on Linux, elsewhere RSS is used directly')
def test_get_used_memory_stops_asking_for_pss_once_it_is_known_to_be_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A system without PSS is asked for it only once, not on every sample of every process."""
    call_count = 0

    def memory_full_info(_process: psutil.Process) -> object:
        nonlocal call_count
        call_count += 1
        return SimpleNamespace(rss=1024)

    monkeypatch.setattr(psutil.Process, 'memory_full_info', memory_full_info)
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=1024))

    for _ in range(3):
        assert system._get_used_memory(psutil.Process()) == 1024

    assert call_count == 1


@pytest.mark.parametrize(
    ('error', 'expected_warning'),
    [
        pytest.param(psutil.AccessDenied(pid=1001), True, id='access denied'),
        pytest.param(psutil.NoSuchProcess(pid=1001), False, id='no such process'),
        pytest.param(psutil.ZombieProcess(pid=1001), False, id='zombie process'),
        pytest.param(FileNotFoundError('/proc/1001/smaps'), True, id='proc entry missing'),
    ],
)
@pytest.mark.usefixtures('measured_current_process')
def test_get_memory_info_skips_children_that_cannot_be_measured(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    *,
    expected_warning: bool,
) -> None:
    """A child that cannot be measured is left out of the sum without hiding the children measured after it."""
    children = [FakeProcess(pid=1001, used_memory=error), FakeProcess(pid=1002, used_memory=40)]
    monkeypatch.setattr(psutil.Process, 'children', lambda *_args, **_kwargs: children)

    with caplog.at_level(logging.WARNING, logger=system.logger.name):
        memory_info = get_memory_info()

    assert memory_info.current_size == ByteSize(140)

    # A child that exits mid-iteration is business as usual, an uninspectable one hides its memory usage.
    warnings = [record.getMessage() for record in caplog.records if 'child process' in record.getMessage()]
    assert bool(warnings) == expected_warning


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(psutil.AccessDenied(), id='access denied'),
        pytest.param(psutil.NoSuchProcess(pid=1), id='no such process'),
        pytest.param(FileNotFoundError('/proc/1/stat'), id='proc entry missing'),
    ],
)
@pytest.mark.usefixtures('measured_current_process')
def test_get_memory_info_handles_failure_to_list_children(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, error: Exception
) -> None:
    """Failure to list the child processes does not abort the whole memory snapshot, but is reported."""

    def raise_error(*_args: object, **_kwargs: object) -> list[psutil.Process]:
        raise error

    monkeypatch.setattr(psutil.Process, 'children', raise_error)

    with caplog.at_level(logging.WARNING, logger=system.logger.name):
        memory_info = get_memory_info()

    assert memory_info.current_size == ByteSize(100)
    assert [record.getMessage() for record in caplog.records if 'child processes' in record.getMessage()]


def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1


@pytest.mark.skipif(sys.platform != 'linux', reason='Improved estimation available only on Linux')
def test_memory_estimation_does_not_overestimate_due_to_shared_memory() -> None:
    """Test that memory usage estimation is not overestimating memory usage by counting shared memory multiple times.

    In this test, the parent process is started and its memory usage is measured in situations where it is running
    child processes without additional memory, with shared additional memory and with own unshared additional memory.
    Child process without additional memory are used to estimate baseline memory usage of any child process.
    The following estimation is asserted by the test:
    additional_memory_size_estimate_per_shared_memory_child * number_of_sharing_children_processes is approximately
    equal to additional_memory_size_estimate_per_unshared_memory_child where the additional shared memory is exactly
    the same as the unshared memory.
    """

    ctx = get_context('fork')
    estimated_memory_expectation = ctx.Value('b', False)  # noqa: FBT003  # Common usage pattern for multiprocessing.Value

    def parent_process() -> None:
        extra_memory_size = 1024 * 1024 * 100  # 100 MB
        children_count = 4
        # Memory calculation is not exact, so allow for some tolerance.
        test_tolerance = 0.3

        def no_extra_memory_child(ready: synchronize.Barrier, measured: synchronize.Barrier) -> None:
            ready.wait()
            measured.wait()

        def extra_memory_child(ready: synchronize.Barrier, measured: synchronize.Barrier) -> None:
            memory = SharedMemory(size=extra_memory_size, create=True)
            assert memory.buf is not None
            memory.buf[:] = bytearray([255 for _ in range(extra_memory_size)])
            print(f'Using the memory... {memory.buf[-1]}')
            ready.wait()
            measured.wait()
            memory.close()
            memory.unlink()

        def shared_extra_memory_child(
            ready: synchronize.Barrier, measured: synchronize.Barrier, memory: SharedMemory
        ) -> None:
            assert memory.buf is not None
            print(f'Using the memory... {memory.buf[-1]}')
            ready.wait()
            measured.wait()

        def get_additional_memory_estimation_while_running_processes(
            *, target: Callable, count: int = 1, use_shared_memory: bool = False
        ) -> float:
            processes = []
            ready = ctx.Barrier(parties=count + 1)
            measured = ctx.Barrier(parties=count + 1)
            shared_memory: SharedMemory | None = None
            memory_before = get_memory_info().current_size

            if use_shared_memory:
                shared_memory = SharedMemory(size=extra_memory_size, create=True)
                assert shared_memory.buf is not None
                shared_memory.buf[:] = bytearray([255 for _ in range(extra_memory_size)])
                extra_args = [shared_memory]
            else:
                extra_args = []

            for _ in range(count):
                p = ctx.Process(target=target, args=[ready, measured, *extra_args])
                p.start()
                processes.append(p)

            ready.wait()
            memory_during = get_memory_info().current_size
            measured.wait()

            for p in processes:
                p.join()

            if shared_memory:
                shared_memory.close()
                shared_memory.unlink()

            return (memory_during - memory_before).to_mb() / count

        additional_memory_simple_child = get_additional_memory_estimation_while_running_processes(
            target=no_extra_memory_child, count=children_count
        )
        additional_memory_extra_memory_child = (
            get_additional_memory_estimation_while_running_processes(target=extra_memory_child, count=children_count)
            - additional_memory_simple_child
        )
        additional_memory_shared_extra_memory_child = (
            get_additional_memory_estimation_while_running_processes(
                target=shared_extra_memory_child, count=children_count, use_shared_memory=True
            )
            - additional_memory_simple_child
        )

        memory_estimation_difference_ratio = (
            abs((additional_memory_shared_extra_memory_child * children_count) - additional_memory_extra_memory_child)
            / additional_memory_extra_memory_child
        )

        estimated_memory_expectation.value = memory_estimation_difference_ratio < test_tolerance

        if not estimated_memory_expectation.value:
            print(
                f'{additional_memory_shared_extra_memory_child=}\n'
                f'{children_count=}\n'
                f'{additional_memory_extra_memory_child=}\n'
                f'{memory_estimation_difference_ratio=}'
            )

    process = ctx.Process(target=parent_process)
    process.start()
    process.join()

    assert estimated_memory_expectation.value, (
        'Estimated memory usage for process with shared memory does not meet the expectation.'
    )
