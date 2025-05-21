from __future__ import annotations

import gc
import os
import time
from multiprocessing import Array, Process, Value
from typing import TYPE_CHECKING

import psutil

from crawlee._utils.byte_size import ByteSize
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized, SynchronizedArray


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert ByteSize(0) < memory_info.total_size < ByteSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1


def _get_test_array() -> SynchronizedArray[int]:
    """Prepare a suitable array size for a test.

    Too small array will make this test useless as we need this array to occupy obvious part of process memory and
    not be hidden in the noise of used memory size.
    """
    return Array('i', range(1000000))


def _child_function(shared_value: SynchronizedArray | None = None) -> SynchronizedArray:
    array = _get_test_array() if shared_value is None else shared_value
    time.sleep(3)
    return array


def _parent_function(ratio: Synchronized) -> None:
    shared_array = _get_test_array()

    sharing_children = [Process(target=_child_function, args=(shared_array,)) for _ in range(10)]
    for child in sharing_children:
        child.start()
    memory_when_sharing_children = get_memory_info()
    for child in sharing_children:
        child.join()
    del sharing_children
    gc.collect()

    nonsharing_children = [Process(target=_child_function) for _ in range(10)]
    for child in nonsharing_children:
        child.start()
    memory_when_nonsharing_children = get_memory_info()
    for child in nonsharing_children:
        child.join()

    # DEBUG in CI
    current_process = psutil.Process(os.getpid())

    ratio.value = repr(current_process.memory_full_info()).encode("utf-8")


def test_memory_measurement_of_shared_memory() -> None:
    """Test that memory usage estimation is not overestimating memory usage by counting shared memory multiple times.

    In this test, the parent process is started and its memory usage is measured. It starts two groups of children
    processes, one group at a time. It measures used memory while all the processes in the group are running. The first
    group (sharing_children) consists of processes that all use the same array from shared memory. The other group
    (nonsharing_children) consists of processes that all use unique array. The overall estimated memory usage of the
    parent process should be significantly lower when sharing_children are running compared to when nonsharing_children
    are running."""

    # The threshold for memory usage ratio of unshared_used_memory processes and shared_used_memory processes
    good_enough_threshold = 0.5

    shared_vs_nonshared_used_memory_ratio: Synchronized = Value('c', b' '*10000)

    process = Process(target=_parent_function, args=(shared_vs_nonshared_used_memory_ratio,))
    process.start()
    process.join()

    raise Exception(shared_vs_nonshared_used_memory_ratio.value.decode("utf-8"))
    assert shared_vs_nonshared_used_memory_ratio.value < good_enough_threshold
