from __future__ import annotations

import sys
from multiprocessing import get_context, synchronize
from multiprocessing.shared_memory import SharedMemory
from typing import TYPE_CHECKING

import pytest

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from collections.abc import Callable
    from multiprocessing.sharedctypes import Synchronized

# The `spawn` start method is used for all processes in the shared-memory estimation test. Unlike `fork`, it is safe
# to use from the multi-threaded pytest process (forking a multi-threaded process is deprecated since Python 3.12),
# but it requires all process targets to be picklable, module-level functions.
_ctx = get_context('spawn')

_EXTRA_MEMORY_SIZE = 1024 * 1024 * 100  # 100 MB


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert ByteSize(0) < memory_info.total_size < ByteSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1


def _no_extra_memory_child(ready: synchronize.Barrier, measured: synchronize.Barrier) -> None:
    ready.wait()
    measured.wait()


def _extra_memory_child(ready: synchronize.Barrier, measured: synchronize.Barrier) -> None:
    memory = SharedMemory(size=_EXTRA_MEMORY_SIZE, create=True)
    assert memory.buf is not None
    memory.buf[:] = bytearray([255 for _ in range(_EXTRA_MEMORY_SIZE)])
    print(f'Using the memory... {memory.buf[-1]}')
    ready.wait()
    measured.wait()
    memory.close()
    memory.unlink()


def _shared_extra_memory_child(ready: synchronize.Barrier, measured: synchronize.Barrier, memory: SharedMemory) -> None:
    assert memory.buf is not None
    print(f'Using the memory... {memory.buf[-1]}')
    ready.wait()
    measured.wait()


def _get_additional_memory_estimation_while_running_processes(
    *, target: Callable, count: int = 1, use_shared_memory: bool = False
) -> float:
    processes = []
    ready = _ctx.Barrier(parties=count + 1)
    measured = _ctx.Barrier(parties=count + 1)
    shared_memory: None | SharedMemory = None
    memory_before = get_memory_info().current_size

    if use_shared_memory:
        shared_memory = SharedMemory(size=_EXTRA_MEMORY_SIZE, create=True)
        assert shared_memory.buf is not None
        shared_memory.buf[:] = bytearray([255 for _ in range(_EXTRA_MEMORY_SIZE)])
        extra_args = [shared_memory]
    else:
        extra_args = []

    for _ in range(count):
        p = _ctx.Process(target=target, args=[ready, measured, *extra_args])
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


def _parent_process(estimated_memory_expectation: Synchronized) -> None:
    children_count = 4
    # Memory calculation is not exact, so allow for some tolerance.
    test_tolerance = 0.3

    additional_memory_simple_child = _get_additional_memory_estimation_while_running_processes(
        target=_no_extra_memory_child, count=children_count
    )
    additional_memory_extra_memory_child = (
        _get_additional_memory_estimation_while_running_processes(target=_extra_memory_child, count=children_count)
        - additional_memory_simple_child
    )
    additional_memory_shared_extra_memory_child = (
        _get_additional_memory_estimation_while_running_processes(
            target=_shared_extra_memory_child, count=children_count, use_shared_memory=True
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
    estimated_memory_expectation = _ctx.Value('b', False)  # noqa: FBT003  # Common usage pattern for multiprocessing.Value

    process = _ctx.Process(target=_parent_process, args=(estimated_memory_expectation,))
    process.start()
    process.join()

    assert estimated_memory_expectation.value, (
        'Estimated memory usage for process with shared memory does not meet the expectation.'
    )
