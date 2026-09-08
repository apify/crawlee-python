from __future__ import annotations

import logging
import sys
import threading
from multiprocessing import get_context, synchronize
from multiprocessing.shared_memory import SharedMemory
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import cgroups_sensor
import psutil
import pytest

from crawlee._utils import system
from crawlee._utils.byte_size import ByteSize
from crawlee._utils.log import LoggerOnce
from crawlee._utils.system import get_cpu_info, get_memory_info

if TYPE_CHECKING:
    from collections.abc import Callable

HOST_TOTAL_BYTES = 8 * 1024**3
HOST_AVAILABLE_BYTES = 3 * 1024**3


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


def raise_no_such_process(process: psutil.Process) -> object:
    """Stand in for a memory metric of a process that exits in the middle of being measured."""
    raise psutil.NoSuchProcess(pid=process.pid)


def fill_buffer(buffer: memoryview, size: int) -> None:
    """Fill the first `size` bytes of a shared memory buffer, one chunk at a time.

    Building the payload as a single object would double the peak memory usage of every process that fills a buffer,
    which is enough for the memory estimation below to be thrown off by page reclaim on a loaded machine.
    """
    chunk_size = 1024 * 1024
    chunk = b'\xff' * chunk_size

    for offset in range(0, size, chunk_size):
        end = min(offset + chunk_size, size)
        buffer[offset:end] = chunk[: end - offset]


@pytest.fixture(autouse=True)
def _isolated_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the process-wide state of the module, so that dedup keys and the latches do not leak between tests."""
    monkeypatch.setattr(system, 'logger_once', LoggerOnce(system.logger))
    monkeypatch.setattr(system._PssAvailability, 'is_available', True)
    monkeypatch.setattr(system._ResourceLimits, 'is_pending', True)


@pytest.fixture(autouse=True)
def cpu_load(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Replace the CPU readings taken against a limit, so that neither a leaked reading nor a real limit is measured."""
    sampler = Mock(spec=cgroups_sensor.CpuLoad)
    # What all three report where nothing restricts the CPU, which sends `get_cpu_info` to the psutil fallback.
    sampler.sample.return_value = None
    monkeypatch.setattr(system, '_cpu_load', sampler)
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_used_ratio', Mock(return_value=None))
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_limit', Mock(return_value=None))
    return sampler


@pytest.fixture
def _cpu_limited(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a CPU limit, so that the load is measured against it rather than against the host machine."""
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_limit', Mock(return_value=1.0))


@pytest.fixture
def measured_current_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the real current process report a fixed memory usage of 100 bytes, whichever metric is used."""
    monkeypatch.setattr(psutil.Process, 'memory_full_info', lambda _process: SimpleNamespace(pss=100))
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=100))


@pytest.fixture
def _fixed_host_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the host memory `psutil` reports, so the expected values do not move with the machine running the tests."""
    monkeypatch.setattr(
        psutil,
        'virtual_memory',
        Mock(return_value=SimpleNamespace(total=HOST_TOTAL_BYTES, available=HOST_AVAILABLE_BYTES)),
    )


def fake_snapshot(
    *, memory_budget: cgroups_sensor.MemoryBudget | None = None, cpu_limit: float | None = None
) -> cgroups_sensor.Snapshot:
    """Stand in for `cgroups_sensor.snapshot()`, describing an unrestricted process unless told otherwise."""
    return cgroups_sensor.Snapshot(memory_budget=memory_budget, cpu_limit=cpu_limit, cpu_usage=None)


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert ByteSize(0) < memory_info.total_size < ByteSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


@pytest.mark.skipif(sys.platform != 'linux', reason='PSS is read only on Linux, elsewhere RSS is used directly')
@pytest.mark.parametrize(
    ('memory_full_info', 'expected_size', 'expected_warning', 'pss_stays_available'),
    [
        pytest.param(raise_access_denied, 2048, True, True, id='access denied'),
        pytest.param(lambda _process: SimpleNamespace(rss=1024, pss=0), 1024, False, True, id='pss reported as zero'),
        pytest.param(lambda _process: SimpleNamespace(rss=1024), 1024, True, False, id='pss field missing'),
    ],
)
def test_get_used_memory_falls_back_to_rss_when_pss_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    memory_full_info: Callable[[psutil.Process], object],
    expected_size: int,
    *,
    expected_warning: bool,
    pss_stays_available: bool,
) -> None:
    """An unreadable PSS metric falls back to the RSS of the same process instead of raising or reporting zero."""
    monkeypatch.setattr(psutil.Process, 'memory_full_info', memory_full_info)
    # A value distinct from the RSS of the full result shows that the metric is re-read only when it has to be -
    # a full result that was read successfully already carries the RSS.
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=2048))

    with caplog.at_level(logging.WARNING, logger=system.logger.name):
        assert system._get_used_memory(psutil.Process()) == expected_size

    # Silently switching the metric would leave an overestimated memory usage unexplained, so both a denial and a
    # system without PSS are reported. An empty `smaps` parses to zero for a process on its way out, which is not.
    warnings = [record.getMessage() for record in caplog.records if 'PSS' in record.getMessage()]
    assert bool(warnings) == expected_warning
    # Only a system that has no PSS at all is latched - a single process denying it says nothing about the others.
    assert system._PssAvailability.is_available == pss_stays_available


@pytest.mark.skipif(sys.platform != 'linux', reason='PSS is read only on Linux, elsewhere RSS is used directly')
def test_get_used_memory_does_not_report_a_vanished_process_as_a_pss_denial(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A process that exits mid-measurement propagates rather than being reported as refusing to expose PSS."""
    monkeypatch.setattr(psutil.Process, 'memory_full_info', raise_no_such_process)
    monkeypatch.setattr(psutil.Process, 'memory_info', raise_no_such_process)

    with caplog.at_level(logging.WARNING, logger=system.logger.name), pytest.raises(psutil.NoSuchProcess):
        system._get_used_memory(psutil.Process())

    assert not [record.getMessage() for record in caplog.records if 'PSS' in record.getMessage()]


@pytest.mark.skipif(sys.platform != 'linux', reason='PSS is read only on Linux, elsewhere RSS is used directly')
def test_get_used_memory_stops_asking_for_pss_once_it_is_known_to_be_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A system without PSS is asked for it only once, not on every sample of every process."""
    memory_full_info = Mock(return_value=SimpleNamespace(rss=1024))
    monkeypatch.setattr(psutil.Process, 'memory_full_info', memory_full_info)
    monkeypatch.setattr(psutil.Process, 'memory_info', lambda _process: SimpleNamespace(rss=1024))

    for _ in range(3):
        assert system._get_used_memory(psutil.Process()) == 1024

    memory_full_info.assert_called_once()


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


@pytest.mark.usefixtures('_fixed_host_memory', 'measured_current_process')
def test_get_memory_info_reports_the_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """A limit applying to the process replaces the memory of the host machine."""
    budget = cgroups_sensor.MemoryBudget(limit=512 * 1024**2, used=100 * 1024**2, available=412 * 1024**2)
    monkeypatch.setattr(cgroups_sensor, 'get_memory_budget', Mock(return_value=budget))

    memory_info = get_memory_info()

    assert memory_info.total_size == ByteSize(budget.limit)
    assert memory_info.system_wide_used_size == ByteSize(budget.used)


@pytest.mark.usefixtures('_fixed_host_memory', 'measured_current_process')
def test_get_memory_info_falls_back_to_the_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrestricted process is measured against the memory of the host machine."""
    monkeypatch.setattr(cgroups_sensor, 'get_memory_budget', Mock(return_value=None))

    memory_info = get_memory_info()

    assert memory_info.total_size == ByteSize(HOST_TOTAL_BYTES)
    assert memory_info.system_wide_used_size == ByteSize(HOST_TOTAL_BYTES - HOST_AVAILABLE_BYTES)


@pytest.mark.usefixtures('_cpu_limited')
def test_get_cpu_info_measures_against_the_limit(monkeypatch: pytest.MonkeyPatch, cpu_load: Mock) -> None:
    """A sampled load is reported as it is, without measuring the host machine as well."""
    cpu_load.sample.return_value = 0.5
    cpu_percent = Mock(return_value=42.0)
    monkeypatch.setattr(psutil, 'cpu_percent', cpu_percent)

    assert get_cpu_info().used_ratio == 0.5
    cpu_percent.assert_not_called()


@pytest.mark.usefixtures('_cpu_limited')
def test_get_cpu_info_measures_a_window_when_the_sampler_has_no_reading(
    monkeypatch: pytest.MonkeyPatch, cpu_load: Mock
) -> None:
    """A sampler with nothing to report yet is covered by a short measurement against the same limit."""
    get_cpu_used_ratio = Mock(return_value=0.25)
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_used_ratio', get_cpu_used_ratio)
    cpu_percent = Mock(return_value=42.0)
    monkeypatch.setattr(psutil, 'cpu_percent', cpu_percent)

    assert get_cpu_info().used_ratio == 0.25
    get_cpu_used_ratio.assert_called_once_with(system._CPU_SAMPLE_INTERVAL_SECS)
    # The measurement is refused below 0.01 seconds and nothing on the path catches that, so a window this short
    # would raise in every limited container while a mocked measurement stays happy with it.
    assert system._CPU_SAMPLE_INTERVAL_SECS >= 0.01
    cpu_percent.assert_not_called()
    cpu_load.sample.assert_called_once()


@pytest.mark.parametrize(
    ('sampled', 'measured'),
    [
        pytest.param(0.0, None, id='sampled'),
        pytest.param(None, 0.0, id='measured over a window'),
    ],
)
@pytest.mark.usefixtures('_cpu_limited')
def test_get_cpu_info_reports_an_idle_limit_as_no_load(
    monkeypatch: pytest.MonkeyPatch, cpu_load: Mock, sampled: float | None, measured: float | None
) -> None:
    """An idle limited container reports no load, which is a reading of zero rather than a missing one."""
    cpu_load.sample.return_value = sampled
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_used_ratio', Mock(return_value=measured))
    cpu_percent = Mock(return_value=42.0)
    monkeypatch.setattr(psutil, 'cpu_percent', cpu_percent)

    assert get_cpu_info().used_ratio == 0.0
    cpu_percent.assert_not_called()


def test_get_cpu_info_measures_the_host_without_a_limit(monkeypatch: pytest.MonkeyPatch, cpu_load: Mock) -> None:
    """Without a limit the process competes for the whole machine, and nothing is measured against a limit."""
    get_cpu_used_ratio = Mock(return_value=0.5)
    monkeypatch.setattr(cgroups_sensor, 'get_cpu_used_ratio', get_cpu_used_ratio)
    monkeypatch.setattr(psutil, 'cpu_percent', Mock(return_value=42.0))

    assert get_cpu_info().used_ratio == 0.42
    cpu_load.sample.assert_not_called()
    get_cpu_used_ratio.assert_not_called()


@pytest.mark.parametrize(
    ('memory_budget', 'cpu_limit', 'expected_message'),
    [
        pytest.param(None, None, None, id='unrestricted'),
        pytest.param(
            cgroups_sensor.MemoryBudget(limit=512 * 1024**2, used=100 * 1024**2, available=412 * 1024**2),
            1.0,
            'memory 512.00 MB, CPU 1 core.',
            id='single core',
        ),
        pytest.param(None, 2.5, 'memory unrestricted, CPU 2.5 cores.', id='fractional cores'),
    ],
)
@pytest.mark.usefixtures('measured_current_process')
def test_log_resource_limits_reports_what_applies(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    memory_budget: cgroups_sensor.MemoryBudget | None,
    cpu_limit: float | None,
    expected_message: str | None,
) -> None:
    """A limit that applies is reported as one line, and an unrestricted process is not reported at all."""
    snapshot = fake_snapshot(memory_budget=memory_budget, cpu_limit=cpu_limit)
    monkeypatch.setattr(cgroups_sensor, 'snapshot', Mock(return_value=snapshot))

    with caplog.at_level(logging.INFO, logger=system.logger.name):
        get_memory_info()

    reported = [record.getMessage() for record in caplog.records if 'Resource limits' in record.getMessage()]

    if expected_message is None:
        assert not reported
    else:
        assert [message for message in reported if expected_message in message]


@pytest.mark.usefixtures('measured_current_process')
def test_log_resource_limits_reports_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reading the limits walks the whole hierarchy, so it is not repeated on every sample."""
    snapshot = Mock(return_value=fake_snapshot())
    monkeypatch.setattr(cgroups_sensor, 'snapshot', snapshot)

    get_memory_info()
    get_memory_info()

    snapshot.assert_called_once()


@pytest.mark.usefixtures('measured_current_process')
def test_log_resource_limits_lets_a_failing_sensor_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sensor that raises is not swallowed, and the latch keeps it to the first sample."""
    snapshot = Mock(side_effect=RuntimeError('Nothing to read here.'))
    monkeypatch.setattr(cgroups_sensor, 'snapshot', snapshot)

    with pytest.raises(RuntimeError):
        get_memory_info()

    # The latch is consumed first, so the next sample reports as usual rather than raising again.
    assert get_memory_info().current_size >= ByteSize(100)
    snapshot.assert_called_once()


def test_log_resource_limits_reports_once_when_two_threads_race(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two event managers sampling in their own threads report the limits once between them, not once each."""
    snapshot = Mock(return_value=fake_snapshot())
    monkeypatch.setattr(cgroups_sensor, 'snapshot', snapshot)
    barrier = threading.Barrier(parties=2)

    def report() -> None:
        barrier.wait()
        system._log_resource_limits()

    threads = [threading.Thread(target=report) for _ in range(2)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    snapshot.assert_called_once()


# The estimation is asserted on absolute memory readings, which hold only as long as nothing else on the machine makes
# the kernel reclaim the pages allocated below. Running alongside the other test workers is enough to break that.
@pytest.mark.run_alone
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
            fill_buffer(memory.buf, extra_memory_size)
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
                fill_buffer(shared_memory.buf, extra_memory_size)
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
