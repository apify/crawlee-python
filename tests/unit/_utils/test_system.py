from __future__ import annotations

import gc
import os
import sys
import time
from multiprocessing import Array, Process, Value, Event
from random import randint
from typing import TYPE_CHECKING

import pytest

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
    good_enough_threshold = 0.5

    def parent_process() -> None:
        sleep_time_for_measurement = 10

        def get_test_list() -> SynchronizedArray[int]:
            return list(randint(1,10000) for _ in range(1_000_000))

        def get_test_array() -> SynchronizedArray[int]:
            return Array('i', get_test_list())

        def no_extra_memory_child(ready_event: Event, measured: Event) -> None:
            ready_event.set()
            measured.wait()
            return None

        def extra_memory_child(ready_event: Event, measured: Event) -> SynchronizedArray[int]:
            array = get_test_list()
            ready_event.set()
            measured.wait()
            return array[-1]

        def shared_extra_memory_child(ready_event: Event, measured: Event, shared_array: SynchronizedArray[int]) -> SynchronizedArray[int]:
            ready_event.set()
            measured.wait()
            return shared_array[-1]

        def get_additional_memory_estimation_while_running_processes(*,target: callable, count: int = 1, process_args: list | None = None) -> ByteSize:
            extra_args = process_args or []
            ready_events = []
            processes = []
            measured_event = Event()
            memory_before = get_memory_info().current_size

            for _ in range(count):
                ready_event = Event()
                ready_events.append(ready_event)
                p = Process(target=target, args=[ready_event, measured_event] + extra_args)
                p.start()
                processes.append(p)
                ready_event.wait()

            memory_during = get_memory_info().current_size
            measured_event.set()

            for p in processes:
                p.join()

            return (memory_during - memory_before).to_mb()/count

        children_count = 10
        shared_array = get_test_array()
        additional_memory_size_estimate = ByteSize(sys.getsizeof(shared_array[0]) * len(shared_array)).to_mb()


        additional_memory_simple_child = get_additional_memory_estimation_while_running_processes(
            target=no_extra_memory_child, count=children_count)
        additional_memory_extra_memory_child = get_additional_memory_estimation_while_running_processes(
            target=extra_memory_child, count=children_count)
        additional_memory_shared_extra_memory_child = get_additional_memory_estimation_while_running_processes(
            target=shared_extra_memory_child, count=children_count, process_args=[shared_array])

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


