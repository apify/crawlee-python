from __future__ import annotations

import sys
from multiprocessing import get_context, synchronize
from multiprocessing.shared_memory import SharedMemory
from typing import TYPE_CHECKING

import psutil
import pytest

from crawlee._utils import system
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from collections.abc import Callable


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert ByteSize(0) < memory_info.total_size < ByteSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


def test_get_memory_info_skips_children_with_access_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    """A child process we are not allowed to inspect must be skipped, not abort the whole snapshot.

    In restricted environments (e.g. hardened containers) reading a child's memory can raise
    `psutil.AccessDenied`, which is not a subclass of `psutil.NoSuchProcess` and so was not suppressed.
    """
    child = psutil.Process()  # any process object works; only `_get_used_memory` behavior matters here

    monkeypatch.setattr(psutil.Process, 'children', lambda *_args, **_kwargs: [child])

    def fake_get_used_memory(process: psutil.Process) -> int:
        if process is child:
            raise psutil.AccessDenied(pid=child.pid)
        return 100

    monkeypatch.setattr(system, '_get_used_memory', fake_get_used_memory)

    memory_info = get_memory_info()

    # The unreadable child is skipped, so only the current process (100) is counted.
    assert memory_info.current_size == ByteSize(100)


def test_get_memory_info_falls_back_to_rss_when_current_process_access_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If PSS for the current process is denied, fall back to RSS instead of crashing.

    On Linux `_get_used_memory` reads PSS via `memory_full_info`, which may require elevated privileges.
    """
    monkeypatch.setattr(psutil.Process, 'children', lambda *_args, **_kwargs: [])

    def fake_get_used_memory(process: psutil.Process) -> int:
        raise psutil.AccessDenied(pid=process.pid)

    monkeypatch.setattr(system, '_get_used_memory', fake_get_used_memory)

    memory_info = get_memory_info()

    # RSS of the current process is a positive value below total system memory.
    assert ByteSize(0) < memory_info.current_size < memory_info.total_size


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
            shared_memory: None | SharedMemory = None
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
