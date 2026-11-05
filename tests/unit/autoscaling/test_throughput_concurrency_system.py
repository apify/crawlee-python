from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import timedelta
from itertools import count, pairwise
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, patch

import pytest

from crawlee._types import ConcurrencySettings
from crawlee.autoscaling import ConcurrencyConsumer, ConcurrencySystem, ThroughputConcurrencySystem
from crawlee.autoscaling._snapshotter import Snapshotter
from crawlee.autoscaling._throughput_concurrency_system import (
    _CONSUMER_TTL_TICKS,
    _MAX_SETTLE_TICKS,
    _SHARED,
    _Cohort,
    _Member,
    _ThroughputController,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

pytestmark = pytest.mark.filterwarnings('ignore:The `ThroughputConcurrencySystem` is experimental:UserWarning')

TICK = 10.0

CONSUMER = ConcurrencyConsumer(id='test-system')

FIRST = ConcurrencyConsumer(id='first')

SECOND = ConcurrencyConsumer(id='second')

TASK_IDS = count()

TO_HOLD = (10.0, 10.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0)
"""Window rates that take a system starting at 40 to a hold at 28: a climb that ends at once, then three ties.

The climb ends at once because a doubling at the same rate doubles the median latency, past `retry_latency_margin`.
"""


class FakeClock:
    """A monotonic clock that only moves when a test moves it."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Iterator[FakeClock]:
    """Replace the clock the system module reads, leaving the event loop's own clock alone."""
    fake = FakeClock()
    with patch('crawlee.autoscaling._throughput_concurrency_system.time', new=fake):
        yield fake


@contextmanager
def live_tasks(number: int) -> Generator[None]:
    """Patch how many tasks the system sees running."""
    with patch.object(
        ThroughputConcurrencySystem, 'current_concurrency', new_callable=PropertyMock, return_value=number
    ):
        yield


def idle_status() -> MagicMock:
    """Build a system status that reports an idle machine until a test says otherwise."""
    status = MagicMock()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    status.get_current_system_info.return_value = MagicMock(is_system_idle=True)
    return status


def make_system(
    *,
    cls: type[ThroughputConcurrencySystem] = ThroughputConcurrencySystem,
    desired: int = 40,
    min_concurrency: int = 1,
    max_concurrency: int = 100,
    status: MagicMock | None = None,
    per_consumer: bool = False,
) -> ThroughputConcurrencySystem:
    """Build a system that is never entered, with the load measurement replaced by `status`."""
    settings = ConcurrencySettings(
        min_concurrency=min_concurrency, desired_concurrency=desired, max_concurrency=max_concurrency
    )
    with (
        patch('crawlee.autoscaling._concurrency_system.Snapshotter', return_value=AsyncMock(spec=Snapshotter)),
        patch('crawlee.autoscaling._concurrency_system.SystemStatus', return_value=status or idle_status()),
    ):
        return cls(
            settings, per_consumer=per_consumer, autoscale_interval=timedelta(seconds=TICK), logging_interval=None
        )


def make_cohort(finished: Iterable[float] = (), *, running: Iterable[float] = (), concurrency: int = 10) -> _Cohort:
    """Build a closed window with `finished` durations and members that have run for `running` seconds at time zero."""
    cohort = _Cohort()
    for duration in finished:
        cohort.join(_Member(started_at=0.0, duration=duration), concurrency)
    for age in running:
        cohort.join(_Member(started_at=-age), concurrency)
    return cohort


@contextmanager
def pretend_active() -> Generator[None]:
    """Let a system that was never entered answer capacity queries."""
    with patch.object(ConcurrencySystem, '_raise_if_inactive'):
        yield


@contextmanager
def parent_accepts_bookings() -> Generator[None]:
    """Let every booking through the parent's budget, so a test drives only the measurement."""
    with (
        patch.object(ConcurrencySystem, 'try_register_task_start', return_value=True),
        patch.object(ConcurrencySystem, 'register_task_end'),
    ):
        yield


class Members:
    """Tasks booked against the system that run until the test finishes them."""

    def __init__(self, system: ThroughputConcurrencySystem, task_ids: list[str]) -> None:
        self._system = system
        self._running = task_ids

    def finish(self, number: int) -> None:
        """Finish the next `number` members that are still running, at the clock's current time."""
        for task_id in self._running[:number]:
            self._system.register_task_end(CONSUMER, task_id)
        del self._running[:number]


@contextmanager
def booked_members(system: ThroughputConcurrencySystem, size: int) -> Generator[Members]:
    """Book `size` tasks against the system, and finish whichever are left on exit."""
    with parent_accepts_bookings():
        task_ids = [str(next(TASK_IDS)) for _ in range(size)]
        for task_id in task_ids:
            system.try_register_task_start(CONSUMER, task_id)
        members = Members(system, task_ids)
        try:
            yield members
        finally:
            members.finish(size)


def run_window(system: ThroughputConcurrencySystem, clock: FakeClock, durations: list[float]) -> int:
    """Run one window at the system's level, one member per duration, and return the level the system sets after it."""
    with live_tasks(system.desired_concurrency):
        clock.advance(_MAX_SETTLE_TICKS * TICK)
        system._autoscale()
        started_at = clock.now

        with booked_members(system, len(durations)) as members:
            for duration in sorted(durations):
                clock.now = started_at + duration
                members.finish(1)

        clock.now = started_at + max(TICK, *durations)
        system._autoscale()
        system._autoscale()

    assert system._controllers[_SHARED]._stage == 'settle', 'the window was not read'
    return system.desired_concurrency


def run_windows(system: ThroughputConcurrencySystem, clock: FakeClock, *rates: float) -> list[int]:
    """Run one window per rate, its members taking the duration that delivers the rate, and return each next level."""
    concurrencies = []
    for rate in rates:
        duration = system.desired_concurrency / rate
        concurrencies.append(run_window(system, clock, [duration] * system.min_members))
    return concurrencies


def run_stalled_window(system: ThroughputConcurrencySystem, clock: FakeClock) -> int:
    """Run one window whose members never finish, until patience reads it, and return the level set after it."""
    with live_tasks(system.desired_concurrency):
        clock.advance(_MAX_SETTLE_TICKS * TICK)
        system._autoscale()

        with booked_members(system, system.min_members):
            clock.advance(TICK)
            system._autoscale()
            clock.advance(10_000.0)
            system._autoscale()

    assert system._controllers[_SHARED]._stage == 'settle', 'the window was not read'
    return system.desired_concurrency


BASE_DURATION = 6.0
"""Seconds a task takes against a target serving no more than its capacity."""


def task_duration(concurrency: int, capacity: int) -> float:
    """Seconds a task takes at `concurrency` against a target that slows quadratically past `capacity`."""
    overload = max(0, concurrency - capacity) / capacity
    return BASE_DURATION * (1 + overload**2)


def run_batches(
    system: ThroughputConcurrencySystem, clock: FakeClock, *, capacity: int, minutes: int
) -> list[tuple[int, float]]:
    """Run tasks against the system in batches of its desired concurrency. Return each batch's level and duration.

    A batch of `concurrency` tasks that each take one duration delivers `concurrency` tasks per duration, which is
    Little's law with no scatter, so the run is exact and takes milliseconds.
    """
    batches = list[tuple[int, float]]()
    last_tick = clock.now

    with (
        parent_accepts_bookings(),
        patch.object(ThroughputConcurrencySystem, 'current_concurrency', new_callable=PropertyMock) as live,
    ):
        while clock.now < minutes * 60:
            concurrency = system.desired_concurrency
            duration = task_duration(concurrency, capacity)
            live.return_value = concurrency

            started_at = clock.now
            for _ in range(concurrency):
                clock.now = started_at
                task_id = str(next(TASK_IDS))
                system.try_register_task_start(CONSUMER, task_id)
                clock.advance(duration)
                system.register_task_end(CONSUMER, task_id)
            batches.append((concurrency, duration))

            if clock.now - last_tick >= TICK:
                system._autoscale()
                last_tick = clock.now

    return batches


def trailing_batches(batches: list[tuple[int, float]], minutes: int) -> list[tuple[int, float]]:
    """Get the trailing batches of a run that together span `minutes`."""
    tail = list[tuple[int, float]]()
    for concurrency, duration in reversed(batches):
        if sum(d for _, d in tail) >= minutes * 60:
            break
        tail.append((concurrency, duration))
    return tail


def test_median_waits_for_running() -> None:
    """The median is withheld while a running member could still be shorter than it."""
    cohort = make_cohort([1.0, 2.0, 3.0], running=[0.0])

    assert cohort.median(2.0) is None
    assert cohort.median(5.0) == pytest.approx(6**0.5, rel=0.01)


def test_levels_sit_around_centre() -> None:
    """The levels are the centre, a doubling, and the centre spread up and down."""
    system = make_system(desired=40)

    assert (
        system._controllers[_SHARED]._concurrency_for('centre'),
        system._controllers[_SHARED]._concurrency_for('climb'),
        system._controllers[_SHARED]._concurrency_for('high'),
        system._controllers[_SHARED]._concurrency_for('low'),
    ) == (40, 80, 60, 27)


def test_levels_stay_inside_settings() -> None:
    """No level leaves the configured bounds."""
    assert make_system(desired=90, max_concurrency=100)._controllers[_SHARED]._concurrency_for('high') == 100
    assert make_system(desired=6, min_concurrency=5)._controllers[_SHARED]._concurrency_for('low') == 5


def test_levels_collapse_at_equal_bounds() -> None:
    """With one allowed concurrency every level is that concurrency."""
    system = make_system(desired=5, min_concurrency=5, max_concurrency=5)

    assert {system._controllers[_SHARED]._concurrency_for(phase) for phase in ('centre', 'climb', 'high', 'low')} == {5}


@pytest.mark.parametrize(
    ('desired', 'expected'),
    [
        pytest.param(2, (3, 1), id='two'),
        pytest.param(3, (4, 2), id='three'),
    ],
)
def test_levels_step_at_least_one_task(desired: int, expected: tuple[int, int]) -> None:
    """Near the floor the two levels still differ from the centre by a task."""

    class Narrow(ThroughputConcurrencySystem):
        dither_max = 0.15

    system = make_system(cls=Narrow, desired=desired)

    assert (
        system._controllers[_SHARED]._concurrency_for('high'),
        system._controllers[_SHARED]._concurrency_for('low'),
    ) == expected


def test_paying_climb_doubles_concurrency(clock: FakeClock) -> None:
    """Each window that delivers a quarter more than the last doubles the desired concurrency."""
    system = make_system(desired=10)

    assert run_windows(system, clock, 10.0, 20.0) == [20, 40]


def test_climb_not_paying_drops_below(clock: FakeClock) -> None:
    """A doubling that delivers no more ends the climb at the lower level around 10, which is 10 / 1.5."""
    system = make_system(desired=10)

    assert run_windows(system, clock, 10.0, 10.0) == [20, 7]


@pytest.mark.parametrize(
    ('durations', 'expected'),
    [
        pytest.param([1.0] * 9 + [11.0], 40, id='equal rate, same median'),
        pytest.param([2.0] * 10, 13, id='equal rate, slower median'),
    ],
)
def test_equal_climb_step_measured_again(clock: FakeClock, durations: list[float], expected: int) -> None:
    """A doubling that delivers about as much is measured again at 40, unless its median latency rose by half."""
    system = make_system(desired=10)
    run_windows(system, clock, 10.0, 18.0)

    assert run_window(system, clock, durations) == expected


def test_losing_climb_step_ends_climb(clock: FakeClock) -> None:
    """A doubling that delivers less ends the climb at the lower level around 20, which is 20 / 1.5."""
    system = make_system(desired=10)

    assert run_windows(system, clock, 10.0, 18.0, 12.0) == [20, 40, 13]


def test_climb_reading_cannot_win(clock: FakeClock) -> None:
    """The climb's last reading beating the lower level sends the system to measure the upper level, 30, not to it."""
    system = make_system(desired=10)
    run_windows(system, clock, 10.0, 18.0, 12.0)

    assert run_windows(system, clock, 6.0, 5.0) == [30, 10]


def test_climb_reading_can_tie(clock: FakeClock) -> None:
    """A tie with the climb's last reading steps the centre from 20 to 16, measured at 13 and then 20."""
    system = make_system(desired=10)
    run_windows(system, clock, 10.0, 18.0, 12.0)

    assert run_windows(system, clock, 12.5, 1.0) == [13, 20]


def test_climb_moves_to_measured_level(clock: FakeClock) -> None:
    """A climb step doubles the level it measured, even when the ceiling has lifted since it was set."""
    system = make_system(desired=10)
    with patch.object(system._controllers[_SHARED], '_soft_ceiling', 15):
        measured = run_window(system, clock, [1.0] * 10)

    assert run_windows(system, clock, 18.0) == [2 * measured]


def test_unserved_first_level_ends_climb(clock: FakeClock) -> None:
    """A first window that runs out of patience ends the climb at the lower level around 10 instead of doubling."""
    system = make_system(desired=10)

    with live_tasks(10):
        clock.advance(_MAX_SETTLE_TICKS * TICK)
        system._autoscale()

        with booked_members(system, system.min_members) as members:
            clock.advance(1.0)
            members.finish(7)
            clock.advance(TICK)
            system._autoscale()
            clock.advance(100.0)
            system._autoscale()

    assert system.desired_concurrency == 7


def test_unfilled_level_ends_climb(clock: FakeClock) -> None:
    """A level the system cannot fill ends the climb without reading a window, at the lower level around 20."""
    system = make_system(desired=20)

    with live_tasks(12), patch.object(system._controllers[_SHARED], '_read') as read:
        clock.advance(_MAX_SETTLE_TICKS * TICK)
        system._autoscale()

    assert system.desired_concurrency == 13
    read.assert_not_called()


def test_upper_level_wins(clock: FakeClock) -> None:
    """The upper level 60 winning becomes the centre with the spread halved, so the next lower level is 60 / 1.25."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)

    assert run_windows(system, clock, 130.0) == [48]


def test_turn_halves_spread(clock: FakeClock) -> None:
    """A win from the other side than the last makes 48 the centre with the narrowest spread, so next is 48 / 1.15."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0, 130.0)

    assert run_windows(system, clock, 130.0, 100.0) == [75, 42]


def test_same_side_doubles_spread(clock: FakeClock) -> None:
    """A second win from the same side makes 75 the centre with the spread doubled, so next is 75 / 1.5."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0, 130.0)

    assert run_windows(system, clock, 100.0, 120.0) == [75, 50]


def test_two_large_wins_resume_climb(clock: FakeClock) -> None:
    """A second upper win as large as a paying climb step resumes the climb from 75 by half again."""
    system = make_system(desired=40, max_concurrency=200)
    run_windows(system, clock, 10.0, 10.0, 5.0, 130.0)

    assert run_windows(system, clock, 100.0, 200.0) == [75, 112]


def test_tie_steps_centre_down(clock: FakeClock) -> None:
    """A tie above the narrowest spread steps the centre from 40 to 32, measured at 26 and then 40."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)

    assert run_windows(system, clock, 5.0, 5.0) == [26, 40]


def test_ties_stop_at_minimum(clock: FakeClock) -> None:
    """Ties near `min_concurrency` step the centre down to it and no further, and the level above is still measured."""
    system = make_system(desired=6, min_concurrency=5)

    assert run_windows(system, clock, *[10.0] * 7) == [12, 5, 5, 6, 5, 6, 5]


def test_tie_at_narrowest_spread_holds(clock: FakeClock) -> None:
    """A tie at the narrowest spread holds the level for six windows, then measures below it again."""
    system = make_system(desired=40)

    concurrencies = run_windows(system, clock, *TO_HOLD, *[5.0] * system.hold_cycles_at_peak)

    assert concurrencies[len(TO_HOLD) - 1 :] == [28] * system.hold_cycles_at_peak + [25]


def test_zero_hold_cycles_never_hold(clock: FakeClock) -> None:
    """A subclass can switch the hold off, and ties then keep moving the level."""

    class NeverHolds(ThroughputConcurrencySystem):
        hold_cycles_at_peak = 0

    system = make_system(cls=NeverHolds, desired=40)

    concurrencies = run_windows(system, clock, *TO_HOLD, 5.0, 5.0, 5.0)

    assert all(concurrency != following for concurrency, following in pairwise(concurrencies))


@pytest.mark.parametrize(
    ('rates', 'expected'),
    [
        pytest.param([3.75], 28, id='one window down by a quarter'),
        pytest.param([3.75, 3.75], 25, id='two windows down by a quarter'),
        pytest.param([4.3, 4.3], 28, id='two windows inside the margin'),
        pytest.param([4.1, 4.1], 25, id='two windows just past the margin'),
        pytest.param([2.0], 25, id='one halved window'),
    ],
)
def test_drift_ends_hold(clock: FakeClock, rates: list[float], expected: int) -> None:
    """Two held windows past the margin on one side of the first, or one at half of it, end the hold at 28."""
    system = make_system(desired=40)
    run_windows(system, clock, *TO_HOLD, 5.0)

    assert run_windows(system, clock, *rates)[-1] == expected


def test_unserved_held_window_ends_hold(clock: FakeClock) -> None:
    """A first held window that delivers nothing before patience runs out ends the hold at once."""
    system = make_system(desired=40)
    run_windows(system, clock, *TO_HOLD)

    assert run_stalled_window(system, clock) == 25


def test_held_window_can_lose_early(clock: FakeClock) -> None:
    """A held window is given up once its bound falls below the first held window's rate by the margin."""
    system = make_system(desired=40)
    run_windows(system, clock, *TO_HOLD, 5.0)

    assert (
        system._controllers[_SHARED]._losing_bound(
            make_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40), now=0.0
        )
        is not None
    )
    assert (
        system._controllers[_SHARED]._losing_bound(make_cohort([0.4, 0.4], running=[0.4, 0.4], concurrency=40), now=0.0)
        is None
    )


def test_win_at_ceiling_holds_there(clock: FakeClock) -> None:
    """An upper win at the concurrency limit holds there for three windows, then looks below at 100 / 1.15."""
    system = make_system(desired=90, max_concurrency=100)
    run_windows(system, clock, 10.0, 11.0, 11.0, 5.0)

    assert run_windows(system, clock, 20.0, 20.0, 20.0, 20.0) == [100, 100, 100, 87]


@pytest.mark.parametrize(
    ('cohort', 'finished'),
    [
        pytest.param(make_cohort([1.0] * 8, running=[0.5, 0.5]), True, id='four fifths'),
        pytest.param(make_cohort([1.0] * 7, running=[0.5] * 3), False, id='less'),
        pytest.param(make_cohort(), False, id='empty'),
    ],
)
def test_window_finishes_at_four_fifths(cohort: _Cohort, finished: bool) -> None:  # noqa: FBT001
    """A window is read once four fifths of its members have finished, and never while empty."""
    assert make_system()._controllers[_SHARED]._finished(cohort) is finished


def test_rate_is_littles_law() -> None:
    """The rate is concurrency times finished members over the time they occupied."""
    assert _ThroughputController._rate(make_cohort([2.0] * 4, concurrency=8)) == pytest.approx(4.0)


def test_upper_level_can_lose_early(clock: FakeClock) -> None:
    """An upper level is given up once its bound falls below the lower level's rate by the margin."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)

    assert (
        system._controllers[_SHARED]._losing_bound(
            make_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40), now=0.0
        )
        is not None
    )
    assert (
        system._controllers[_SHARED]._losing_bound(make_cohort([1.0, 1.0], running=[1.0, 1.0], concurrency=40), now=0.0)
        is None
    )


def test_slow_tail_does_not_lose_early(clock: FakeClock) -> None:
    """A slow tail left running does not lose an upper level whose reading, taken without it, would win."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)

    # Eight of ten members make a reading: one more finishing at 30 seconds reads 40 * 8 / 37, above the lower level.
    assert (
        system._controllers[_SHARED]._losing_bound(make_cohort([1.0] * 7, running=[30.0] * 3, concurrency=40), now=0.0)
        is None
    )


def test_young_members_raise_bound(clock: FakeClock) -> None:
    """Running members past the ones a reading needs count toward the bound when they would raise the rate."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)

    # Eight members read 40 * 8 / 71, below the lower level, but all ten read 40 * 10 / 73, above it.
    assert (
        system._controllers[_SHARED]._losing_bound(make_cohort([10.0] * 7, running=[1.0] * 3, concurrency=40), now=0.0)
        is None
    )


def test_climb_step_can_lose_early(clock: FakeClock) -> None:
    """A climb step is given up once its bound falls below the last step's rate by the climb margin."""
    system = make_system(desired=10)
    run_windows(system, clock, 10.0)

    assert (
        system._controllers[_SHARED]._losing_bound(
            make_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=20), now=0.0
        )
        is not None
    )
    assert (
        system._controllers[_SHARED]._losing_bound(make_cohort([0.5, 0.5], running=[0.5, 0.5], concurrency=20), now=0.0)
        is None
    )


def test_finished_window_is_read() -> None:
    """A mostly finished window is read as its rate and sends the system back to settling."""
    system = make_system(desired=40)
    window = make_cohort([1.0] * 8, running=[0.5, 0.5], concurrency=40)

    with (
        patch.object(system._controllers[_SHARED], '_window', window),
        patch.object(system._controllers[_SHARED], '_read') as read,
    ):
        system._controllers[_SHARED]._wait(50.0)

    assert read.call_args.args[0] == pytest.approx(40.0)
    assert system._controllers[_SHARED]._stage_since == 50.0


def test_losing_window_is_read_early(clock: FakeClock) -> None:
    """A window whose bound already loses is read before it finishes, at that bound."""
    system = make_system(desired=40)
    run_windows(system, clock, 10.0, 10.0, 5.0)
    window = make_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40)

    # The running members' ages are counted from time zero.
    with (
        patch.object(system._controllers[_SHARED], '_window', window),
        patch.object(system._controllers[_SHARED], '_read') as read,
    ):
        system._controllers[_SHARED]._wait(0.0)

    # Three of the four members make a reading, so the best case is one running member finishing at its age.
    assert read.call_args.args[0] == pytest.approx(40 * 3 / 32)


def test_out_of_patience_reads_zero() -> None:
    """A window that outlasts its patience reads as a level the target cannot serve."""
    system = make_system(desired=40)
    window = make_cohort([1.0], running=[0.0] * 9, concurrency=40)

    with (
        patch.object(_ThroughputController, '_patience_unit', new_callable=PropertyMock, return_value=10.0),
        patch.object(system._controllers[_SHARED], '_window', window),
        patch.object(system._controllers[_SHARED], '_stage_since', 0.0),
        patch.object(system._controllers[_SHARED], '_read') as read,
    ):
        system._controllers[_SHARED]._wait(51.0)

    assert read.call_args.args[0] == 0.0


@pytest.mark.parametrize(
    ('window_concurrency', 'live', 'filled'),
    [
        pytest.param(40, 40, True, id='filled'),
        pytest.param(20, 40, False, id='unfilled while enrolling'),
        pytest.param(40, 20, False, id='unfilled when read'),
    ],
)
def test_filled_needs_both_readings(window_concurrency: int, live: int, filled: bool) -> None:  # noqa: FBT001
    """A level counts as filled only if it was filled while enrolling and still is when read."""
    system = make_system(desired=40)
    window = make_cohort([1.0] * 10, concurrency=window_concurrency)

    with (
        live_tasks(live),
        patch.object(system._controllers[_SHARED], '_window', window),
        patch.object(system._controllers[_SHARED], '_read'),
    ):
        system._controllers[_SHARED]._wait(0.0)

    assert system._controllers[_SHARED]._filled is filled


def test_reading_error_does_not_stall(clock: FakeClock) -> None:
    """An error while reading a window still leaves the system able to measure the next one."""
    system = make_system(desired=40)
    window = make_cohort([1.0] * 10, concurrency=40)

    with (
        patch.object(system._controllers[_SHARED], '_window', window),
        patch.object(system._controllers[_SHARED], '_read', side_effect=RuntimeError),
        pytest.raises(RuntimeError),
    ):
        system._controllers[_SHARED]._wait(clock.now)

    assert run_windows(system, clock, 10.0) == [80]


@pytest.mark.parametrize(
    ('durations', 'expected'),
    [
        pytest.param([], float('inf'), id='nothing finished'),
        pytest.param([30.0], 30.0, id='recent duration'),
    ],
)
def test_first_window_patience(durations: list[float], expected: float) -> None:
    """Before any level is read, patience follows the recent task duration once there is one."""
    system = make_system(desired=40)

    with patch.object(system._controllers[_SHARED], '_durations', deque(durations)):
        assert system._controllers[_SHARED]._patience_unit == expected


def test_overload_cuts_and_restarts(clock: FakeClock) -> None:
    """An overloaded machine cuts by the parent's step and ends the climb, so a paying window no longer doubles."""
    status = idle_status()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    system = make_system(desired=80, status=status)

    system._autoscale()
    assert system.desired_concurrency == 76

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    [concurrency] = run_windows(system, clock, 100.0)
    assert 76 <= concurrency < 152


def test_overload_ends_peak_hold(clock: FakeClock) -> None:
    """An overload during a hold at the peak ends the hold, so the system measures the levels around the cut again."""
    status = idle_status()
    system = make_system(desired=40, status=status)
    run_windows(system, clock, *TO_HOLD)

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()
    assert system.desired_concurrency == 26

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    assert run_windows(system, clock, 5.0, 5.0) == [32, 30]


def test_overload_forgets_last_win(clock: FakeClock) -> None:
    """An upper win before an overload does not count toward resuming the climb after it, so 54 winning moves to 47."""
    status = idle_status()
    system = make_system(desired=40, status=status)
    run_windows(system, clock, 10.0, 10.0, 5.0, 130.0)

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    assert run_windows(system, clock, 5.0, 100.0) == [54, 47]


@pytest.mark.parametrize(
    ('desired', 'expected'),
    [
        pytest.param(1, 1, id='floor'),
        pytest.param(3, 2, id='three'),
        pytest.param(4, 3, id='four'),
    ],
)
def test_overload_near_floor(desired: int, expected: int) -> None:
    """An overload near the minimum concurrency cuts by one parent step, and at the minimum leaves the system there."""
    status = idle_status()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    system = make_system(desired=desired, status=status)

    system._autoscale()

    assert system.desired_concurrency == expected


def test_overload_undoes_raise(clock: FakeClock) -> None:
    """An overload before the raised level was read returns the system to the level before the raise."""
    status = idle_status()
    system = make_system(desired=40, status=status)
    assert run_windows(system, clock, 10.0) == [80]

    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()

    assert system.desired_concurrency == 40


def test_read_window_confirms_raise(clock: FakeClock) -> None:
    """Once a window at the raised level has been read, an overload no longer undoes the raise."""
    status = idle_status()
    system = make_system(desired=90, max_concurrency=100, status=status)
    assert run_windows(system, clock, 10.0, 11.0, 11.0, 5.0, 20.0) == [100, 100, 60, 100, 100]

    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()

    assert system.desired_concurrency == 100


def test_current_overload_keeps_concurrency() -> None:
    """A current overload with no raise to undo is left to the historical signal."""
    status = idle_status()
    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    system = make_system(desired=40, status=status)

    system._autoscale()

    assert system.desired_concurrency == 40


def test_soft_ceiling_lifts_when_idle() -> None:
    """The cap left by an overload lifts by the parent's step each idle tick."""
    status = idle_status()
    status.get_historical_system_info.side_effect = [MagicMock(is_system_idle=value) for value in (False, True)]
    system = make_system(desired=80, status=status)

    system._autoscale()
    system._autoscale()

    assert system._controllers[_SHARED]._soft_ceiling == 80


def test_empty_window_reopens(clock: FakeClock) -> None:
    """A window nobody joined is opened again rather than read."""
    system = make_system(desired=40)
    controller = system._controllers[_SHARED]

    with live_tasks(40):
        clock.now = 100.0
        system._autoscale()
        first = controller._window

        clock.now = 141.0
        system._autoscale()

    assert controller._stage == 'enrol'
    assert controller._window is not first
    assert controller._window is not None
    assert controller._window.size == 0


def test_window_closes_after_four_spans(clock: FakeClock) -> None:
    """A window with too few members still closes after four spans."""
    system = make_system(desired=40)

    with live_tasks(40):
        clock.now = 100.0
        system._autoscale()

        with booked_members(system, 3):
            clock.now = 141.0
            system._autoscale()

            assert system._controllers[_SHARED]._stage == 'wait'


async def test_entering_restarts_cycle(clock: FakeClock) -> None:
    """Entering drops a window a previous session left open and counts settling from now."""
    system = make_system(desired=40)
    controller = system._controllers[_SHARED]

    with live_tasks(40):
        clock.now = 100.0
        system._autoscale()

    assert controller._stage == 'enrol'

    clock.now = 500.0
    with patch.object(ConcurrencySystem, '__aenter__', new=AsyncMock()):
        await system.__aenter__()

    assert (controller._stage, controller._stage_since, controller._window, controller._enrolling) == (
        'settle',
        500.0,
        None,
        None,
    )


def test_task_end_records_duration(clock: FakeClock) -> None:
    """A finished task joins its window and the recent durations."""
    system = make_system(desired=40)

    with live_tasks(40):
        clock.now = 100.0
        system._autoscale()

    window = system._controllers[_SHARED]._window

    with booked_members(system, 1) as members:
        clock.advance(3.0)
        members.finish(1)

    assert window is not None
    assert (window.size, len(window.finished_durations), window.finished_total) == (1, 1, 3.0)
    assert list(system._controllers[_SHARED]._durations) == [3.0]


def test_quick_task_has_a_rate(clock: FakeClock) -> None:
    """A task too quick for the clock to time still gives its window a rate."""
    system = make_system(desired=40)

    with live_tasks(40):
        clock.now = 100.0
        system._autoscale()

    window = system._controllers[_SHARED]._window

    with live_tasks(40), booked_members(system, 1):
        pass

    assert window is not None
    assert _ThroughputController._rate(window) > 0


def test_settles_near_peak(clock: FakeClock) -> None:
    """Against a target that slows past its capacity, the system settles near the peak and short of its limit."""
    capacity = 40
    system = make_system(desired=10, max_concurrency=100)
    peak = max(range(1, 101), key=lambda concurrency: concurrency / task_duration(concurrency, capacity))

    tail = trailing_batches(run_batches(system, clock, capacity=capacity, minutes=30), minutes=5)

    assert system.desired_concurrency < 100
    assert all(abs(concurrency - peak) <= 0.25 * peak for concurrency, _ in tail)
    delivered = sum(concurrency for concurrency, _ in tail) / sum(duration for _, duration in tail)
    assert delivered >= 0.9 * peak / task_duration(peak, capacity)


def test_holds_at_limit_below_knee(clock: FakeClock) -> None:
    """Against a target that never slows below the limit, the system climbs to the limit and stays there."""
    system = make_system(desired=10, max_concurrency=100)

    tail = trailing_batches(run_batches(system, clock, capacity=1000, minutes=30), minutes=5)

    concurrencies = [concurrency for concurrency, _ in tail]
    assert concurrencies.count(100) >= 0.75 * len(concurrencies)


def test_emits_experimental_warning() -> None:
    """Building the system warns that it is experimental."""
    with pytest.warns(UserWarning, match='experimental'):
        make_system()


def test_tuning_by_subclassing(clock: FakeClock) -> None:
    """Class attributes set on a subclass reach the system."""

    class Wide(ThroughputConcurrencySystem):
        startup_gain = 3.0

    system = make_system(cls=Wide, desired=20)

    assert run_windows(system, clock, 10.0) == [60]


def test_shared_mode_keeps_no_counts() -> None:
    """Without `per_consumer`, booking tasks for several consumers leaves no per-consumer counts behind."""
    system = make_system(desired=2)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')
        assert system.try_register_task_start(SECOND, '1')
        system.register_task_end(FIRST, '1')
        system.register_task_end(SECOND, '1')

    assert not system._consumer_concurrency


def test_consumer_gets_own_controller() -> None:
    """Each consumer gets its own controller at the settings' level, and the system reports their sum."""
    system = make_system(desired=20, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
        system.has_capacity_for_task(SECOND)

    desired = {key: controller.desired_concurrency for key, controller in system._controllers.items()}
    assert desired == {'first': 20, 'second': 20}
    assert system.desired_concurrency == 40


def test_capacity_follows_own_concurrency() -> None:
    """A consumer at its own level is refused while another one still gets capacity."""
    system = make_system(desired=2, per_consumer=True)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')
        assert system.try_register_task_start(FIRST, '2')

        assert not system.has_capacity_for_task(FIRST)
        assert system.has_capacity_for_task(SECOND)


def test_task_end_frees_own_slot() -> None:
    """Ending a task gives its consumer the slot back."""
    system = make_system(desired=1, per_consumer=True)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')
        assert not system.has_capacity_for_task(FIRST)

        system.register_task_end(FIRST, '1')

        assert system.has_capacity_for_task(FIRST)


def test_refused_task_keeps_own_slot() -> None:
    """A task the parent's budget refuses does not take a slot from its consumer."""
    system = make_system(desired=1, per_consumer=True)

    with pretend_active():
        with patch.object(ConcurrencySystem, 'try_register_task_start', return_value=False):
            assert not system.try_register_task_start(FIRST, '1')

        assert system.has_capacity_for_task(FIRST)


def test_unknown_task_end_keeps_slot() -> None:
    """Ending a task that was never booked does not free a slot of its consumer."""
    system = make_system(desired=1, per_consumer=True)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')

        system.register_task_end(FIRST, '2')

        assert not system.has_capacity_for_task(FIRST)


def test_equal_consumers_kept_apart() -> None:
    """Two consumers sharing an id fill one share, yet their tasks booked under the same ids end apart."""
    system = make_system(desired=2, per_consumer=True)
    first = ConcurrencyConsumer(id='shared')
    second = ConcurrencyConsumer(id='shared')

    with pretend_active():
        assert system.try_register_task_start(first, '0')
        assert system.try_register_task_start(second, '0')
        assert not system.has_capacity_for_task(first)

        system.register_task_end(first, '0')
        system.register_task_end(second, '0')

    assert list(system._controllers) == ['shared']
    assert system._consumer_concurrency['shared'] == 0


def test_overload_keeps_own_minimum() -> None:
    """On an overloaded machine a consumer still starts tasks while it runs fewer than `min_concurrency`."""
    status = idle_status()
    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    system = make_system(desired=4, min_concurrency=2, per_consumer=True, status=status)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')
        assert system.try_register_task_start(FIRST, '2')

        assert not system.has_capacity_for_task(FIRST)


def test_no_consumer_checks_total() -> None:
    """Asking without a consumer is gated by the system's total, while a consumer is gated by its own level."""
    system = make_system(desired=1, per_consumer=True)

    with pretend_active():
        assert system.try_register_task_start(FIRST, '1')

        assert not system.has_capacity_for_task()
        assert system.has_capacity_for_task(SECOND)


@pytest.mark.parametrize(
    ('wants', 'min_concurrency', 'maximum', 'expected'),
    [
        pytest.param((100, 100), 1, 60, 30, id='both over the budget'),
        pytest.param((10, 100), 1, 60, 50, id='one under an equal share'),
        pytest.param((10, 20), 1, 60, 60, id='both fit'),
        pytest.param((100, 100, 100), 3, 6, 3, id='minimum per consumer'),
    ],
)
def test_budget_split(wants: tuple[int, ...], min_concurrency: int, maximum: int, expected: int) -> None:
    """Each consumer gets an equal share, grown by what the others do not want, never below `min_concurrency`."""
    system = make_system(
        desired=min_concurrency, min_concurrency=min_concurrency, max_concurrency=maximum, per_consumer=True
    )
    controllers = {f'consumer {index}': Mock(wanted=want) for index, want in enumerate(wants)}

    with patch.object(system, '_controllers', controllers):
        system._share_budget()

    assert {c.limit_to.call_args.args[0] for c in controllers.values()} == {expected}


def test_newcomer_cuts_shares_at_once() -> None:
    """A consumer joining a full budget cuts the level of the one already there to its share right away."""
    system = make_system(desired=20, max_concurrency=30, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
        system.has_capacity_for_task(SECOND)

    assert [controller.desired_concurrency for controller in system._controllers.values()] == [15, 15]


@pytest.mark.parametrize(
    'per_consumer',
    [
        pytest.param(False, id='shared'),
        pytest.param(True, id='per consumer'),
    ],
)
def test_lower_max_cuts_concurrency_at_once(per_consumer: bool) -> None:  # noqa: FBT001
    """Lowering `max_concurrency` while the system runs cuts the level right away."""
    system = make_system(desired=20, max_concurrency=30, per_consumer=per_consumer)

    with pretend_active():
        system.has_capacity_for_task(FIRST)

    system.max_concurrency = 10

    assert system.desired_concurrency == 10


@pytest.mark.parametrize(
    'per_consumer',
    [
        pytest.param(False, id='shared'),
        pytest.param(True, id='per consumer'),
    ],
)
def test_higher_min_lifts_concurrency_at_once(per_consumer: bool) -> None:  # noqa: FBT001
    """Raising `min_concurrency` while the system runs lifts the level right away."""
    system = make_system(desired=20, max_concurrency=30, per_consumer=per_consumer)

    with pretend_active():
        system.has_capacity_for_task(FIRST)

    system.min_concurrency = 25

    assert system.desired_concurrency == 25


def test_no_consumer_desired_in_bounds() -> None:
    """With no consumer yet, lowering `max_concurrency` below the start level still bounds the desired concurrency."""
    system = make_system(desired=20, max_concurrency=30, per_consumer=True)

    system.max_concurrency = 10

    assert system.desired_concurrency == 10


@pytest.mark.parametrize(
    ('in_flight', 'kept'),
    [
        pytest.param(False, False, id='idle'),
        pytest.param(True, True, id='task in flight'),
    ],
)
def test_gone_consumer_dropped(clock: FakeClock, in_flight: bool, kept: bool) -> None:  # noqa: FBT001
    """A consumer that stopped asking for capacity is dropped after the TTL, unless it still has tasks running."""
    system = make_system(desired=10, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
        if in_flight:
            system.try_register_task_start(FIRST, '1')

    clock.advance(_CONSUMER_TTL_TICKS * TICK + 1)
    system._autoscale()

    assert (FIRST.id in system._controllers) is kept


def test_gone_consumer_frees_share(clock: FakeClock) -> None:
    """A dropped consumer's share goes back to the others on the next tick."""
    system = make_system(desired=20, max_concurrency=30, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
        system.has_capacity_for_task(SECOND)

    clock.advance(_CONSUMER_TTL_TICKS * TICK + 1)
    with pretend_active():
        system.has_capacity_for_task(SECOND)
    system._autoscale()

    assert system._controllers[SECOND.id].bound == 30


def test_window_counts_own_tasks() -> None:
    """A consumer's window counts the running tasks of that consumer alone."""
    system = make_system(desired=10, per_consumer=True)
    window = _Cohort()

    with pretend_active():
        system.try_register_task_start(SECOND, '1')
        system.try_register_task_start(SECOND, '2')
        system.has_capacity_for_task(FIRST)
        with patch.object(system._controllers[FIRST.id], '_enrolling', window):
            system.try_register_task_start(FIRST, '1')

    assert window.concurrency == 1


def test_share_cut_drops_raise() -> None:
    """A share cut below a recorded raise leaves no raise for an overload to undo."""
    system = make_system(desired=20, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
    controller = system._controllers[FIRST.id]

    with patch.object(controller, '_concurrency_before_raise', 15):
        controller.limit_to(10)

        assert (controller.desired_concurrency, controller._concurrency_before_raise) == (10, None)


def test_concurrency_in_budget_after_tick() -> None:
    """Controllers that move up within a tick are cut back to the shared budget before anyone books."""
    system = make_system(desired=30, max_concurrency=60, per_consumer=True)

    with pretend_active():
        system.has_capacity_for_task(FIRST)
        system.has_capacity_for_task(SECOND)

    def climb(controller: _ThroughputController) -> None:
        controller.desired_concurrency *= 2

    with (
        patch.object(_ThroughputController, 'tick', autospec=True, side_effect=climb),
        patch.object(_ThroughputController, 'wanted', new_callable=PropertyMock, return_value=60),
    ):
        system._autoscale()

    assert system.desired_concurrency == 60


def test_full_budget_refuses_above_min() -> None:
    """A consumer above its minimum may not start a task while the system already runs `max_concurrency`."""
    system = make_system(desired=10, max_concurrency=10, per_consumer=True)

    with pretend_active():
        for task_id in range(10):
            assert system.try_register_task_start(FIRST, str(task_id))

        # The newcomer cuts the first consumer to a share of 5, but its ten tasks keep running.
        assert system.try_register_task_start(SECOND, '0')
        assert not system.has_capacity_for_task(SECOND)


def test_max_wins_over_raised_min() -> None:
    """With `min_concurrency` raised above `max_concurrency` at runtime, the level stays at the maximum."""
    status = idle_status()
    system = make_system(desired=40, status=status)
    system.max_concurrency = 5
    system.min_concurrency = 10

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()

    assert system.desired_concurrency == 5


def test_undo_stays_above_raised_min(clock: FakeClock) -> None:
    """An overload undoing a raise recorded before `min_concurrency` grew stops at the minimum, once."""
    status = idle_status()
    system = make_system(desired=10, status=status)
    assert run_windows(system, clock, 10.0) == [20]

    system.min_concurrency = 15
    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    system._autoscale()

    assert system.desired_concurrency == 15
    assert system._controllers[_SHARED]._concurrency_before_raise is None


def test_newcomer_starts_inside_bounds() -> None:
    """A consumer joining after `min_concurrency` grew starts at the minimum without a raise to undo."""
    system = make_system(desired=4, per_consumer=True)
    system.min_concurrency = 10

    with pretend_active():
        system.has_capacity_for_task(FIRST)
    controller = system._controllers[FIRST.id]

    assert (controller.desired_concurrency, controller._concurrency_before_raise) == (10, None)
