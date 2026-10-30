# ruff: noqa: FBT003 # Boolean positional value in function call

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, TypeVar, cast
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from crawlee import ConcurrencySettings
from crawlee.autoscaling import (
    AutoscaledPool,
    ConcurrencyConsumer,
    ConcurrencySystem,
    LoadRatioInfo,
    LoadSignalsOptions,
    LoadSignalStartContext,
    LoadSnapshot,
    SystemInfo,
)
from crawlee.autoscaling._load_signal import LoadSignal
from crawlee.configuration import Configuration
from tests.unit.utils import poll_until_condition

if TYPE_CHECKING:
    from collections.abc import Awaitable


T = TypeVar('T')

CONSUMER = ConcurrencyConsumer(id='test-pool')


def future(value: T, /) -> Awaitable[T]:
    """Wrap a value in an already resolved future, for the pool's synchronous predicates."""
    f = asyncio.Future[T]()
    f.set_result(value)
    return f


def system_info(*, overloaded: bool = False) -> SystemInfo:
    """Build a system info reporting an idle system, or one whose memory is overloaded."""
    return SystemInfo(
        cpu_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
        memory_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=1.0 if overloaded else 0.3),
        event_loop_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
        storage_client_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
    )


def load_signal(name: str) -> Mock:
    """Build a custom load signal that reports an idle resource."""
    signal = Mock(spec=LoadSignal)
    signal.name = name
    signal.overloaded_ratio = 0.5
    signal.get_sample.return_value = []
    return signal


def only_custom_signals(*signals: LoadSignal) -> LoadSignalsOptions:
    """Switch the built-in signals off, leaving only `signals` to watch."""
    return LoadSignalsOptions(memory=False, event_loop=False, cpu=False, storage_client=False, custom=signals)


@pytest.mark.parametrize(
    ('kwargs', 'error'),
    [
        pytest.param({'autoscale_interval': timedelta(0)}, 'autoscale_interval must be positive', id='autoscale'),
        pytest.param({'logging_interval': timedelta(0)}, 'logging_interval must be positive', id='logging'),
        pytest.param({'snapshot_history': timedelta(0)}, 'snapshot_history must be positive', id='snapshot_history'),
        pytest.param({'current_history': timedelta(0)}, 'current_history must be positive', id='current_history'),
        pytest.param({'desired_concurrency_ratio': 0}, 'desired_concurrency_ratio must be between', id='desired'),
        pytest.param({'desired_concurrency_ratio': 1}, 'desired_concurrency_ratio must be between', id='desired_one'),
        pytest.param({'scale_up_step_ratio': 0}, 'scale_up_step_ratio must be between', id='scale_up'),
        pytest.param({'scale_up_step_ratio': 1.5}, 'scale_up_step_ratio must be between', id='scale_up_over_one'),
        pytest.param({'scale_down_step_ratio': -0.1}, 'scale_down_step_ratio must be between', id='scale_down'),
    ],
)
def test_rejects_invalid_options(kwargs: dict, error: str) -> None:
    """An option outside its range is refused up front."""
    with pytest.raises(ValueError, match=error):
        ConcurrencySystem(**kwargs)


def test_configures_load_measurement() -> None:
    """The signal options, the windows and the configuration reach the components that measure the load."""
    configuration = Configuration()
    custom_signal = load_signal('proxy_health')
    load_signals = LoadSignalsOptions(cpu=False, custom=[custom_signal])

    with (
        patch('crawlee.autoscaling._concurrency_system.Snapshotter') as snapshotter_class,
        patch('crawlee.autoscaling._concurrency_system.SystemStatus') as system_status_class,
    ):
        ConcurrencySystem(
            load_signals=load_signals,
            current_history=timedelta(seconds=2),
            snapshot_history=timedelta(seconds=20),
            configuration=configuration,
        )

    assert snapshotter_class.call_args == call(
        load_signals,
        max_sample_window=timedelta(seconds=20),
        configuration=configuration,
    )
    assert system_status_class.call_args.kwargs == {
        'current_history': timedelta(seconds=2),
        'snapshot_history': timedelta(seconds=20),
    }


@pytest.mark.parametrize(
    ('load_signals', 'error'),
    [
        pytest.param(
            LoadSignalsOptions(custom=[load_signal('cpu_info')]),
            r"Duplicate load signal name 'cpu_info': .* `LoadSignalsOptions\(cpu=False\)`",
            id='builtin',
        ),
        pytest.param(
            LoadSignalsOptions(custom=[load_signal('proxy_health'), load_signal('proxy_health')]),
            "Duplicate load signal name 'proxy_health': two custom signals cannot share a name",
            id='custom',
        ),
    ],
)
def test_rejects_duplicate_signal_names(load_signals: LoadSignalsOptions, error: str) -> None:
    """Signal names key the reported status, so two signals cannot share one."""
    with pytest.raises(ValueError, match=error):
        ConcurrencySystem(load_signals=load_signals)


async def test_custom_signal_lifecycle() -> None:
    """Custom signals start with the widest window and the system's configuration, and stop with the system."""
    signal = load_signal('proxy_health')
    configuration = Configuration()
    concurrency_system = ConcurrencySystem(
        load_signals=only_custom_signals(signal),
        current_history=timedelta(seconds=40),
        configuration=configuration,
    )

    async with concurrency_system:
        context = LoadSignalStartContext(max_sample_window=timedelta(seconds=40), configuration=configuration)
        signal.start.assert_awaited_once_with(context)
        signal.stop.assert_not_awaited()

    signal.stop.assert_awaited_once()


async def test_failed_signal_start_rolls_back() -> None:
    """A signal that fails to start is stopped along with the ones started before it, and the system stays inactive."""
    started_signal = load_signal('first')
    failing_signal = load_signal('second')
    failing_signal.start.side_effect = RuntimeError('Signal failure')
    concurrency_system = ConcurrencySystem(load_signals=only_custom_signals(started_signal, failing_signal))

    with pytest.raises(RuntimeError, match='Signal failure'):
        await concurrency_system.__aenter__()

    assert not concurrency_system.active
    started_signal.stop.assert_awaited_once()
    failing_signal.stop.assert_awaited_once()


async def test_custom_signal_overload_holds_tasks() -> None:
    """An overloaded custom signal counts as an overloaded system."""
    signal = load_signal('proxy_health')
    signal.get_sample.return_value = [LoadSnapshot(is_overloaded=True)]
    settings = ConcurrencySettings(min_concurrency=1, desired_concurrency=2, max_concurrency=2)

    async with ConcurrencySystem(settings, load_signals=only_custom_signals(signal)) as concurrency_system:
        # Below the minimum, a task starts regardless of the load.
        assert concurrency_system.try_register_task_start(CONSUMER, '1')
        assert not concurrency_system.has_capacity_for_task(CONSUMER)
        assert concurrency_system.get_current_status().load_signal_info['proxy_health'].is_overloaded


async def test_cannot_be_entered_twice(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """One lifecycle per system, so a second entry is refused instead of restarting the monitoring."""
    async with make_concurrency_system() as concurrency_system:
        assert concurrency_system.active

        with pytest.raises(RuntimeError, match='already active'):
            await concurrency_system.__aenter__()

    assert not concurrency_system.active


async def test_exit_stops_all_despite_failure(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """A cancellation arriving mid-shutdown must not leave anything running behind an inactive system."""
    concurrency_system = make_concurrency_system(ConcurrencySettings(max_tasks_per_minute=10))
    await concurrency_system.__aenter__()

    log_task = concurrency_system._log_system_status_task
    assert log_task is not None

    with (
        patch.object(concurrency_system._autoscale_task, 'stop', AsyncMock(side_effect=asyncio.CancelledError)),
        pytest.raises(asyncio.CancelledError),
    ):
        await concurrency_system.__aexit__(None, None, None)

    assert concurrency_system._age_tasks_started_task.task is not None
    assert concurrency_system._age_tasks_started_task.task.cancelled()
    assert log_task.task is not None
    assert log_task.task.cancelled()
    cast('AsyncMock', concurrency_system._snapshotter).__aexit__.assert_awaited_once()


async def test_rolls_back_failed_entry(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A half-entered system is nobody's to exit, so whatever it did start is torn down right away."""
    concurrency_system = make_concurrency_system()
    log_task = concurrency_system._log_system_status_task
    assert log_task is not None

    with (
        patch.object(log_task, 'start', side_effect=RuntimeError('Logging failure')),
        pytest.raises(RuntimeError, match='Logging failure'),
    ):
        await concurrency_system.__aenter__()

    assert not concurrency_system.active
    assert concurrency_system._autoscale_task.task is not None
    assert concurrency_system._autoscale_task.task.cancelled()
    cast('AsyncMock', concurrency_system._snapshotter).__aexit__.assert_awaited_once()


async def test_refuses_queries_while_inactive(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """Nothing measures the load or scales the budget then, so there is nothing to answer from."""
    concurrency_system = make_concurrency_system()

    with pytest.raises(RuntimeError, match='not active'):
        concurrency_system.has_capacity_for_task(CONSUMER)

    with pytest.raises(RuntimeError, match='not active'):
        concurrency_system.try_register_task_start(CONSUMER, '0')

    with pytest.raises(RuntimeError, match='not active'):
        concurrency_system.get_current_status()


async def test_releases_slot_while_inactive(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A task settling after its system is gone still returns its slot."""
    concurrency_system = make_concurrency_system()

    async with concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')

    concurrency_system.register_task_end(CONSUMER, '0')

    assert concurrency_system.current_concurrency == 0


async def test_no_capacity_at_desired(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A spent budget refuses capacity until a booked task returns its slot."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=4, desired_concurrency=1)

    async with make_concurrency_system(settings) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')
        assert concurrency_system.current_concurrency == 1

        assert not concurrency_system.has_capacity_for_task(CONSUMER)
        assert not concurrency_system.try_register_task_start(CONSUMER, '1')

        concurrency_system.register_task_end(CONSUMER, '0')
        assert concurrency_system.current_concurrency == 0
        assert concurrency_system.has_capacity_for_task(CONSUMER)


async def test_evaluates_load_once_per_task(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """The pre-check and the booking that follows it share one load verdict."""
    async with make_concurrency_system() as concurrency_system:
        assert concurrency_system.has_capacity_for_task(CONSUMER)
        assert concurrency_system.try_register_task_start(CONSUMER, '0')

    assert cast('Mock', system_status.get_current_system_info).call_count == 1


async def test_refuses_unbooked_release(
    make_concurrency_system: type[ConcurrencySystem],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A release without a booking is refused, so the budget cannot go negative and stop refusing tasks."""
    async with make_concurrency_system() as concurrency_system:
        with caplog.at_level(logging.WARNING, logger='crawlee.autoscaling._concurrency_system'):
            concurrency_system.register_task_end(CONSUMER, '0')

        assert concurrency_system.current_concurrency == 0
        assert concurrency_system.has_capacity_for_task(CONSUMER)

    assert 'never started' in caplog.text


async def test_capacity_without_consumer(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """Capacity can be queried without naming a consumer."""
    async with make_concurrency_system() as concurrency_system:
        assert concurrency_system.has_capacity_for_task()


@pytest.mark.parametrize(
    'logging_interval',
    [
        pytest.param(timedelta(seconds=0.05), id='enabled'),
        pytest.param(None, id='disabled'),
    ],
)
async def test_logging_interval_controls_log(
    logging_interval: timedelta | None,
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The state is logged at DEBUG on the configured interval, and `None` turns the logging off."""
    cast('Mock', system_status.get_historical_system_info).side_effect = system_info

    with caplog.at_level(logging.DEBUG, logger='crawlee.autoscaling._concurrency_system'):
        async with make_concurrency_system(logging_interval=logging_interval):
            await asyncio.sleep(0.2)

    state_records = [record for record in caplog.records if 'current concurrency' in record.message]
    assert bool(state_records) is (logging_interval is not None)
    assert all(record.levelno == logging.DEBUG for record in state_records)


async def test_reports_current_load(make_concurrency_system: type[ConcurrencySystem], system_status: Mock) -> None:
    """The reported load is the verdict that gates task dispatch."""
    overloaded = system_info(overloaded=True)
    cast('Mock', system_status.get_current_system_info).return_value = overloaded

    async with make_concurrency_system() as concurrency_system:
        assert concurrency_system.get_current_status() is overloaded


async def test_scales_up_and_down(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """An idle system grows the budget up to `max_concurrency`, and an overloaded one takes it back down."""
    done_count = 0

    async def run(_task_id: str) -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    overload_active = False

    cast('Mock', system_status.get_historical_system_info).side_effect = lambda: system_info(overloaded=overload_active)

    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=4, desired_concurrency=1)

    async with make_concurrency_system(settings, autoscale_interval=timedelta(seconds=0.1)) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(True),
            is_finished_function=lambda: future(False),
        )

        pool_run_task = asyncio.create_task(pool.run(), name='pool run task')

        try:
            # Wait until concurrency scales up above 1.
            assert await poll_until_condition(lambda: concurrency_system.desired_concurrency > 1, timeout=5.0)

            # Wait until concurrency reaches maximum.
            assert await poll_until_condition(lambda: concurrency_system.desired_concurrency == 4, timeout=5.0)

            # Multiple concurrent workers should have completed more tasks than a single worker could.
            assert await poll_until_condition(lambda: done_count > 10, timeout=5.0)

            # Simulate memory overload and wait for the system to scale down.
            overload_active = True
            assert await poll_until_condition(lambda: concurrency_system.desired_concurrency < 4, timeout=5.0)

            # Wait until it scales all the way down to minimum.
            assert await poll_until_condition(lambda: concurrency_system.desired_concurrency == 1, timeout=5.0)
        finally:
            pool_run_task.cancel()
            with suppress(asyncio.CancelledError):
                await pool_run_task


async def test_ratio_holds_back_scale_up(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """Concurrency does not grow until the ratio of the desired concurrency is running."""
    cast('Mock', system_status.get_historical_system_info).side_effect = system_info

    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=40, desired_concurrency=20)

    async with make_concurrency_system(
        settings,
        desired_concurrency_ratio=0.95,
        autoscale_interval=timedelta(seconds=0.1),
    ) as concurrency_system:
        # Eighteen tasks would reach the default ratio of 0.9, but not 0.95.
        for task_id in range(18):
            assert concurrency_system.try_register_task_start(CONSUMER, str(task_id))

        await asyncio.sleep(0.5)

        assert concurrency_system.desired_concurrency == 20


async def test_keeps_min_when_overloaded(make_concurrency_system: type[ConcurrencySystem], system_status: Mock) -> None:
    """`min_concurrency` tasks keep running while the system stays overloaded."""
    done_count = 0
    running_count = 0
    max_running_count = 0

    async def run(_task_id: str) -> None:
        nonlocal done_count, running_count, max_running_count
        running_count += 1
        max_running_count = max(max_running_count, running_count)
        await asyncio.sleep(0.05)
        running_count -= 1
        done_count += 1

    overloaded = system_info(overloaded=True)
    cast('Mock', system_status.get_current_system_info).return_value = overloaded
    cast('Mock', system_status.get_historical_system_info).return_value = overloaded

    settings = ConcurrencySettings(min_concurrency=2, max_concurrency=4, desired_concurrency=4)

    async with make_concurrency_system(settings) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(done_count < 5),
            is_finished_function=lambda: future(done_count >= 5),
        )

        await asyncio.wait_for(pool.run(), timeout=5)

    assert done_count >= 5
    assert max_running_count == 2


async def test_drops_to_min_on_overload(make_concurrency_system: type[ConcurrencySystem], system_status: Mock) -> None:
    """An overload starting mid-run settles concurrency at `min_concurrency`."""
    done_count = 0
    running_count = 0
    max_running_count = 0
    overloaded = False

    async def run(_task_id: str) -> None:
        nonlocal done_count, running_count, max_running_count
        running_count += 1
        max_running_count = max(max_running_count, running_count)
        await asyncio.sleep(0.05)
        running_count -= 1
        done_count += 1

    cast('Mock', system_status.get_current_system_info).side_effect = lambda: system_info(overloaded=overloaded)
    cast('Mock', system_status.get_historical_system_info).side_effect = lambda: system_info(overloaded=overloaded)

    settings = ConcurrencySettings(min_concurrency=2, max_concurrency=4, desired_concurrency=4)

    async with make_concurrency_system(settings) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(True),
            is_finished_function=lambda: future(False),
        )

        pool_run_task = asyncio.create_task(pool.run(), name='pool run task')

        try:
            assert await poll_until_condition(lambda: max_running_count == 4, timeout=5.0)

            overloaded = True

            # The tasks started while the system was idle have to finish before the overload cap becomes observable.
            assert await poll_until_condition(lambda: concurrency_system.current_concurrency <= 2, timeout=5.0)

            max_running_count = 0
            done_before = done_count
            assert await poll_until_condition(lambda: done_count >= done_before + 4, timeout=5.0)

            assert max_running_count == 2
        finally:
            pool_run_task.cancel()
            with suppress(asyncio.CancelledError):
                await pool_run_task


async def test_capacity_ignores_per_minute_cap(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """The cap belongs to the booking, so the pre-check keeps reporting capacity and an empty queue is never stalled."""
    settings = ConcurrencySettings(min_concurrency=5, max_concurrency=5, desired_concurrency=5, max_tasks_per_minute=1)

    async with make_concurrency_system(settings) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')
        concurrency_system.register_task_end(CONSUMER, '0')

        assert concurrency_system.has_capacity_for_task(CONSUMER)
        assert not concurrency_system.try_register_task_start(CONSUMER, '1')


async def test_per_minute_cap_frees_with_age(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """The cap counts the last minute, so the oldest second's tasks stop counting once it falls out of the window."""
    settings = ConcurrencySettings(
        min_concurrency=5,
        max_concurrency=5,
        desired_concurrency=5,
        max_tasks_per_minute=2,
    )

    async with make_concurrency_system(settings) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')
        assert concurrency_system.try_register_task_start(CONSUMER, '1')
        assert not concurrency_system.try_register_task_start(CONSUMER, '2')

        for task_id in ('0', '1'):
            concurrency_system.register_task_end(CONSUMER, task_id)

        # A booking lands in the newest of the sixty buckets (as in TS), so it keeps counting for fifty-nine more ticks.
        for _ in range(59):
            concurrency_system._age_tasks_started()

        assert not concurrency_system.try_register_task_start(CONSUMER, '2')

        concurrency_system._age_tasks_started()
        assert concurrency_system.try_register_task_start(CONSUMER, '2')


async def test_reentry_resets_per_minute_cap(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A restarted system does not count the task starts of its previous session."""
    concurrency_system = make_concurrency_system(ConcurrencySettings(max_tasks_per_minute=1))

    async with concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')
        concurrency_system.register_task_end(CONSUMER, '0')

    async with concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '1')


async def test_no_scale_up_over_per_minute_cap(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """A rate-limited run has no idle capacity to judge, so the concurrency stays where it is."""
    cast('Mock', system_status.get_historical_system_info).side_effect = system_info

    settings = ConcurrencySettings(
        min_concurrency=1,
        max_concurrency=10,
        desired_concurrency=1,
        max_tasks_per_minute=1,
    )

    async with make_concurrency_system(settings, autoscale_interval=timedelta(seconds=0.1)) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')

        await asyncio.sleep(0.5)

        assert concurrency_system.desired_concurrency == 1


async def test_scales_down_over_per_minute_cap(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """An overloaded machine is relieved even while the run is held back by the per-minute cap."""
    cast('Mock', system_status.get_historical_system_info).side_effect = lambda: system_info(overloaded=True)

    settings = ConcurrencySettings(
        min_concurrency=1,
        max_concurrency=10,
        desired_concurrency=10,
        max_tasks_per_minute=1,
    )

    async with make_concurrency_system(settings, autoscale_interval=timedelta(seconds=0.1)) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')

        assert await poll_until_condition(lambda: concurrency_system.desired_concurrency < 10, timeout=5.0)


@pytest.mark.parametrize(
    'bound',
    [
        pytest.param('min_concurrency', id='min'),
        pytest.param('max_concurrency', id='max'),
    ],
)
@pytest.mark.parametrize(
    'value',
    [
        pytest.param(0, id='zero'),
        pytest.param(-1, id='negative'),
    ],
)
async def test_rejects_invalid_retuned_bound(
    bound: str,
    value: int,
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """A retuned bound has to be at least 1."""
    concurrency_system = make_concurrency_system()

    with pytest.raises(ValueError, match=f'{bound} must be at least 1'):
        setattr(concurrency_system, bound, value)


async def test_lower_max_pulls_desired_down(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """Dispatch gates on the desired concurrency, so a lowered ceiling has to take it with it."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=10, desired_concurrency=10)
    concurrency_system = make_concurrency_system(settings)

    concurrency_system.max_concurrency = 4

    assert concurrency_system.max_concurrency == 4
    assert concurrency_system.desired_concurrency == 4


async def test_higher_min_pushes_desired_up(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """The minimum is kept up regardless of the load, so the budget follows it."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=10, desired_concurrency=2)
    concurrency_system = make_concurrency_system(settings)

    concurrency_system.min_concurrency = 6

    assert concurrency_system.desired_concurrency == 6


async def test_contradictory_bounds_favour_max(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """The maximum is the limit set to protect something, so it wins over a minimum raised above it."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=4, desired_concurrency=2)
    concurrency_system = make_concurrency_system(settings)

    concurrency_system.min_concurrency = 10

    assert concurrency_system.desired_concurrency == 4


async def test_scales_past_raised_max(
    make_concurrency_system: type[ConcurrencySystem],
    system_status: Mock,
) -> None:
    """A ceiling raised mid-run is picked up by the next autoscale tick."""
    cast('Mock', system_status.get_historical_system_info).side_effect = system_info

    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=2, desired_concurrency=2)

    async with make_concurrency_system(settings, autoscale_interval=timedelta(seconds=0.1)) as concurrency_system:
        assert concurrency_system.try_register_task_start(CONSUMER, '0')
        assert concurrency_system.try_register_task_start(CONSUMER, '1')

        concurrency_system.max_concurrency = 8

        assert await poll_until_condition(lambda: concurrency_system.desired_concurrency > 2, timeout=5.0)
