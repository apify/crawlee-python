from __future__ import annotations

import gc
import os
import sys
import time
from multiprocessing import Array, Process, Value, Event, Barrier, heap
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.managers import SharedMemoryManager
from random import randint
from typing import TYPE_CHECKING

import psutil
import pytest
from anyio import sleep

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


@pytest.mark.skipif(os.name == 'nt', reason='Improved estimation not available on Windows')
def test_memory_estimation_does_not_overestimate_due_to_shared_memory() -> None:
    """Test that memory usage estimation is not overestimating memory usage by counting shared memory multiple times.

    In this test, the parent process is started and its memory usage is measured. It starts two groups of children
    processes, one group at a time. It measures used memory while all the processes in the group are running. The first
    group (sharing_children) consists of processes that all use the same array from shared memory. The other group
    (nonsharing_children) consists of processes that all use unique array. The overall estimated memory usage of the
    parent process should be significantly lower when sharing_children are running compared to when nonsharing_children
    are running."""

    # The threshold for memory usage ratio of unshared_used_memory processes and shared_used_memory processes

    def parent_process() -> None:
        extra_memory_size = 1024*1024*10  # 1 MB shared memory
        children_count = 1


        def no_extra_memory_child(ready: Barrier, measured: Barrier) -> None:
            ready.wait()
            measured.wait()
            return None

        def extra_memory_child(ready: Barrier, measured: Barrier) -> None:
            current_process = psutil.Process(os.getpid())
            before = current_process.memory_full_info()
            memory = SharedMemory(size=extra_memory_size, create=True)
            during = current_process.memory_full_info()
            memory.buf[:] = bytearray([1 for _ in range(extra_memory_size)])
            during2 = current_process.memory_full_info()
            ready.wait()
            measured.wait()
            memory.close()
            memory.unlink()
            after = current_process.memory_full_info()
            print("")

        def shared_extra_memory_child(ready: Barrier, measured: Barrier, memory: SharedMemory) -> None:
            ready.wait()
            measured.wait()


        def get_additional_memory_estimation_while_running_processes(*,target: callable, count: int = 1, use_shared_memory: bool = False) -> ByteSize:
            ready_events = []
            processes = []
            ready = Barrier(parties=count+1)
            measured_event = Barrier(parties=count+1)
            memory_before = get_memory_info().current_size
            shared_memory = None

            if use_shared_memory:
                shared_memory = SharedMemory(size=extra_memory_size, create=True)
                shared_memory.buf[:] = bytearray([1 for _ in range(extra_memory_size)])
                extra_args = [shared_memory]
            else:
                extra_args = []

            for _ in range(count):
                ready_event = Event()
                ready_events.append(ready_event)
                p = Process(target=target, args=[ready, measured_event] + extra_args)
                p.start()
                processes.append(p)

            ready.wait()

            memory_during = get_memory_info().current_size
            measured_event.wait()

            for p in processes:
                p.join()

            if shared_memory:
                shared_memory.close()
                shared_memory.unlink()

            return (memory_during - memory_before).to_mb()/count



        additional_memory_simple_child = get_additional_memory_estimation_while_running_processes(
            target=no_extra_memory_child, count=children_count)
        additional_memory_shared_extra_memory_child = get_additional_memory_estimation_while_running_processes(
            target=shared_extra_memory_child, count=children_count, use_shared_memory=True) - additional_memory_simple_child
        additional_memory_extra_memory_child = get_additional_memory_estimation_while_running_processes(
            target=extra_memory_child, count=children_count) - additional_memory_simple_child


        """
        # This is not exact measurement, just estimation. Allow for some tolerance.
        t1 =estimated_additional_memory_per_shared_extra_memory_child < (
            (additional_memory_size_estimate/children_count) * 1.1)
        t2 =estimated_additional_memory_per_shared_extra_memory_child > (
            (additional_memory_size_estimate/children_count) * 0.9)

        t3=estimated_additional_memory_per_extra_memory_child < (
            (estimated_additional_memory_per_extra_memory_child) * 1.1)
        t4 = estimated_additional_memory_per_extra_memory_child > (
            (estimated_additional_memory_per_extra_memory_child) * 0.9)
        """

        print("uiii")

    process = Process(target=parent_process)
    process.start()
    process.join()


