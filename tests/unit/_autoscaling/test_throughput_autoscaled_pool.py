from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from itertools import pairwise
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from crawlee._autoscaling import AutoscaledPool, ThroughputAutoscaledPool
from crawlee._autoscaling.throughput_autoscaled_pool import _MAX_SETTLE_TICKS, _Cohort
from crawlee._types import ConcurrencySettings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

pytestmark = pytest.mark.filterwarnings('ignore:The `ThroughputAutoscaledPool` is experimental:UserWarning')

_TICK = AutoscaledPool._AUTOSCALE_INTERVAL.total_seconds()

_TO_HOLD = (10.0, 10.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0)
"""Window rates that take a pool starting at 40 to a hold at 28: a climb that ends at once, then three ties.

The climb ends at once because a doubling at the same rate doubles the median latency, past `retry_latency_margin`.
"""


class _Clock:
    """A monotonic clock that only moves when a test moves it."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Iterator[_Clock]:
    """Replace the clock the pool module reads, leaving the event loop's own clock alone."""
    fake = _Clock()
    with patch('crawlee._autoscaling.throughput_autoscaled_pool.time', new=fake):
        yield fake


@contextmanager
def _live(count: int) -> Iterator[None]:
    """Patch how many tasks the pool sees running."""
    with patch.object(ThroughputAutoscaledPool, 'current_concurrency', new_callable=PropertyMock, return_value=count):
        yield


def _status() -> MagicMock:
    """Build a system status that reports an idle machine until a test says otherwise."""
    status = MagicMock()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    status.get_current_system_info.return_value = MagicMock(is_system_idle=True)
    return status


def _pool(
    *,
    cls: type[ThroughputAutoscaledPool] = ThroughputAutoscaledPool,
    desired: int = 40,
    min_concurrency: int = 1,
    max_concurrency: int = 100,
    status: MagicMock | None = None,
) -> ThroughputAutoscaledPool:
    """Build a pool that is never run."""
    return cls(
        system_status=status or _status(),
        concurrency_settings=ConcurrencySettings(
            min_concurrency=min_concurrency, desired_concurrency=desired, max_concurrency=max_concurrency
        ),
        run_task_function=MagicMock(),
        is_task_ready_function=MagicMock(),
        is_finished_function=MagicMock(),
    )


def _cohort(finished: Iterable[float] = (), *, running: Iterable[float] = (), concurrency: int = 10) -> _Cohort:
    """Build a closed window with `finished` durations and members that have run for `running` seconds at time zero."""
    cohort = _Cohort()
    for duration in finished:
        cohort.leave(cohort.join(0.0, concurrency), duration)
    for age in running:
        cohort.join(-age, concurrency)
    return cohort


class _Members:
    """Worker tasks of the pool that run until the test finishes them."""

    def __init__(self, gates: list[asyncio.Event], tasks: list[asyncio.Task]) -> None:
        self._gates = gates
        self._tasks = tasks

    async def finish(self, count: int) -> None:
        """Finish the next `count` members that are still running, at the clock's current time."""
        chosen = [(gate, task) for gate, task in zip(self._gates, self._tasks, strict=True) if not gate.is_set()]
        for gate, _ in chosen[:count]:
            gate.set()
        await asyncio.gather(*(task for _, task in chosen[:count]))


@asynccontextmanager
async def _members(pool: ThroughputAutoscaledPool, count: int) -> AsyncIterator[_Members]:
    """Start `count` of the pool's own worker tasks, and finish whichever are left on exit."""
    gates = [asyncio.Event() for _ in range(count)]
    waiting = iter(gates)

    async def run() -> None:
        await next(waiting).wait()

    with patch.object(AutoscaledPool, '_worker_task', new=AsyncMock(side_effect=run)):
        tasks = [asyncio.create_task(pool._worker_task()) for _ in range(count)]
        await asyncio.sleep(0)
        members = _Members(gates, tasks)
        try:
            yield members
        finally:
            await members.finish(count)


async def _window(pool: ThroughputAutoscaledPool, clock: _Clock, durations: list[float]) -> int:
    """Run one window at the pool's level, one member per duration, and return the level the pool sets after it."""
    with _live(pool.desired_concurrency):
        clock.advance(_MAX_SETTLE_TICKS * _TICK)
        pool._autoscale()
        started_at = clock.now

        async with _members(pool, len(durations)) as members:
            for duration in sorted(durations):
                clock.now = started_at + duration
                await members.finish(1)

        clock.now = started_at + max(_TICK, *durations)
        pool._autoscale()
        pool._autoscale()

    assert pool._stage == 'settle', 'the window was not read'
    return pool.desired_concurrency


async def _levels(pool: ThroughputAutoscaledPool, clock: _Clock, *rates: float) -> list[int]:
    """Run one window per rate, its members taking the duration that delivers the rate, and return each next level."""
    levels = []
    for rate in rates:
        duration = pool.desired_concurrency / rate
        levels.append(await _window(pool, clock, [duration] * pool.min_members))
    return levels


async def _stalled_window(pool: ThroughputAutoscaledPool, clock: _Clock) -> int:
    """Run one window whose members never finish, until patience reads it, and return the level set after it."""
    with _live(pool.desired_concurrency):
        clock.advance(_MAX_SETTLE_TICKS * _TICK)
        pool._autoscale()

        async with _members(pool, pool.min_members):
            clock.advance(_TICK)
            pool._autoscale()
            clock.advance(10_000.0)
            pool._autoscale()

    assert pool._stage == 'settle', 'the window was not read'
    return pool.desired_concurrency


_BASE_DURATION = 6.0
"""Seconds a task takes against a target serving no more than its capacity."""


def _duration(concurrency: int, capacity: int) -> float:
    """Seconds a task takes at `concurrency` against a target that slows quadratically past `capacity`."""
    overload = max(0, concurrency - capacity) / capacity
    return _BASE_DURATION * (1 + overload**2)


async def _run(
    pool: ThroughputAutoscaledPool, clock: _Clock, *, capacity: int, minutes: int
) -> list[tuple[int, float]]:
    """Run the pool's own worker tasks in batches of its desired concurrency. Return each batch's level and duration.

    A batch of `level` tasks that each take one duration delivers `level` tasks per duration, which is Little's law
    with no scatter, so the run is exact and takes milliseconds.
    """
    batches = list[tuple[int, float]]()
    last_tick = clock.now

    with (
        patch.object(AutoscaledPool, '_worker_task', new=AsyncMock()) as task,
        patch.object(ThroughputAutoscaledPool, 'current_concurrency', new_callable=PropertyMock) as live,
    ):
        while clock.now < minutes * 60:
            level = pool.desired_concurrency
            duration = _duration(level, capacity)
            live.return_value = level
            task.side_effect = partial(clock.advance, duration)

            started_at = clock.now
            for _ in range(level):
                clock.now = started_at
                await pool._worker_task()
            batches.append((level, duration))

            if clock.now - last_tick >= _TICK:
                pool._autoscale()
                last_tick = clock.now

    return batches


def _tail(batches: list[tuple[int, float]], minutes: int) -> list[tuple[int, float]]:
    """Get the trailing batches of a run that together span `minutes`."""
    tail = list[tuple[int, float]]()
    for level, duration in reversed(batches):
        if sum(d for _, d in tail) >= minutes * 60:
            break
        tail.append((level, duration))
    return tail


def test_median_waits_for_running_members() -> None:
    """The median is withheld while a running member could still be shorter than it."""
    cohort = _cohort([1.0, 2.0, 3.0], running=[0.0])

    assert cohort.median(2.0) is None
    assert cohort.median(5.0) == pytest.approx(6**0.5, rel=0.01)


def test_levels_sit_around_the_centre() -> None:
    """The levels are the centre, a doubling, and the centre spread up and down."""
    pool = _pool(desired=40)

    assert (pool._level('centre'), pool._level('climb'), pool._level('high'), pool._level('low')) == (40, 80, 60, 27)


def test_levels_stay_inside_the_settings() -> None:
    """No level leaves the configured bounds."""
    assert _pool(desired=90, max_concurrency=100)._level('high') == 100
    assert _pool(desired=6, min_concurrency=5)._level('low') == 5


def test_levels_collapse_when_min_equals_max() -> None:
    """With one allowed concurrency every level is that concurrency."""
    pool = _pool(desired=5, min_concurrency=5, max_concurrency=5)

    assert {pool._level(phase) for phase in ('centre', 'climb', 'high', 'low')} == {5}


@pytest.mark.parametrize(
    ('desired', 'expected'),
    [
        pytest.param(2, (3, 1), id='two'),
        pytest.param(3, (4, 2), id='three'),
    ],
)
def test_levels_step_at_least_one_task(desired: int, expected: tuple[int, int]) -> None:
    """Near the floor the two levels still differ from the centre by a task."""

    class Narrow(ThroughputAutoscaledPool):
        dither_max = 0.15

    pool = _pool(cls=Narrow, desired=desired)

    assert (pool._level('high'), pool._level('low')) == expected


async def test_paying_climb_doubles_the_concurrency(clock: _Clock) -> None:
    """Each window that delivers a quarter more than the last doubles the desired concurrency."""
    pool = _pool(desired=10)

    assert await _levels(pool, clock, 10.0, 20.0) == [20, 40]


async def test_climb_that_stops_paying_drops_below_it(clock: _Clock) -> None:
    """A doubling that delivers no more ends the climb at the lower level around 10, which is 10 / 1.5."""
    pool = _pool(desired=10)

    assert await _levels(pool, clock, 10.0, 10.0) == [20, 7]


@pytest.mark.parametrize(
    ('durations', 'expected'),
    [
        pytest.param([1.0] * 9 + [11.0], 40, id='equal rate, same median'),
        pytest.param([2.0] * 10, 13, id='equal rate, slower median'),
    ],
)
async def test_equal_climb_step_is_measured_again(clock: _Clock, durations: list[float], expected: int) -> None:
    """A doubling that delivers about as much is measured again at 40, unless its median latency rose by half."""
    pool = _pool(desired=10)
    await _levels(pool, clock, 10.0, 18.0)

    assert await _window(pool, clock, durations) == expected


async def test_losing_climb_step_ends_the_climb(clock: _Clock) -> None:
    """A doubling that delivers less ends the climb at the lower level around 20, which is 20 / 1.5."""
    pool = _pool(desired=10)

    assert await _levels(pool, clock, 10.0, 18.0, 12.0) == [20, 40, 13]


async def test_climb_reading_cannot_win(clock: _Clock) -> None:
    """The climb's last reading beating the lower level sends the pool to measure the upper level, 30, not to it."""
    pool = _pool(desired=10)
    await _levels(pool, clock, 10.0, 18.0, 12.0)

    assert await _levels(pool, clock, 6.0, 5.0) == [30, 10]


async def test_climb_reading_can_tie(clock: _Clock) -> None:
    """A tie with the climb's last reading steps the centre from 20 to 16, measured at 13 and then 20."""
    pool = _pool(desired=10)
    await _levels(pool, clock, 10.0, 18.0, 12.0)

    assert await _levels(pool, clock, 12.5, 1.0) == [13, 20]


async def test_climb_moves_to_the_measured_level(clock: _Clock) -> None:
    """A climb step doubles the level it measured, even when the ceiling has lifted since it was set."""
    pool = _pool(desired=10)
    with patch.object(pool, '_soft_ceiling', 15):
        measured = await _window(pool, clock, [1.0] * 10)

    assert await _levels(pool, clock, 18.0) == [2 * measured]


async def test_unserved_first_level_ends_the_climb(clock: _Clock) -> None:
    """A first window that runs out of patience ends the climb at the lower level around 10 instead of doubling."""
    pool = _pool(desired=10)

    with _live(10):
        clock.advance(_MAX_SETTLE_TICKS * _TICK)
        pool._autoscale()

        async with _members(pool, pool.min_members) as members:
            clock.advance(1.0)
            await members.finish(7)
            clock.advance(_TICK)
            pool._autoscale()
            clock.advance(100.0)
            pool._autoscale()

    assert pool.desired_concurrency == 7


async def test_unfilled_level_ends_the_climb(clock: _Clock) -> None:
    """A level the pool cannot fill ends the climb without reading a window, at the lower level around 20."""
    pool = _pool(desired=20)

    with _live(12), patch.object(pool, '_read') as read:
        clock.advance(_MAX_SETTLE_TICKS * _TICK)
        pool._autoscale()

    assert pool.desired_concurrency == 13
    read.assert_not_called()


async def test_upper_level_wins(clock: _Clock) -> None:
    """The upper level 60 winning becomes the centre with the spread halved, so the next lower level is 60 / 1.25."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)

    assert await _levels(pool, clock, 130.0) == [48]


async def test_turn_halves_the_spread(clock: _Clock) -> None:
    """A win from the other side than the last makes 48 the centre with the narrowest spread, so next is 48 / 1.15."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0, 130.0)

    assert await _levels(pool, clock, 130.0, 100.0) == [75, 42]


async def test_same_side_doubles_the_spread(clock: _Clock) -> None:
    """A second win from the same side makes 75 the centre with the spread doubled, so next is 75 / 1.5."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0, 130.0)

    assert await _levels(pool, clock, 100.0, 120.0) == [75, 50]


async def test_two_large_wins_resume_the_climb(clock: _Clock) -> None:
    """A second upper win as large as a paying climb step resumes the climb from 75 by half again."""
    pool = _pool(desired=40, max_concurrency=200)
    await _levels(pool, clock, 10.0, 10.0, 5.0, 130.0)

    assert await _levels(pool, clock, 100.0, 200.0) == [75, 112]


async def test_tie_steps_the_centre_down(clock: _Clock) -> None:
    """A tie above the narrowest spread steps the centre from 40 to 32, measured at 26 and then 40."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)

    assert await _levels(pool, clock, 5.0, 5.0) == [26, 40]


async def test_ties_stop_at_the_minimum(clock: _Clock) -> None:
    """Ties near `min_concurrency` step the centre down to it and no further, and the level above is still measured."""
    pool = _pool(desired=6, min_concurrency=5)

    assert await _levels(pool, clock, *[10.0] * 7) == [12, 5, 5, 6, 5, 6, 5]


async def test_tie_at_narrowest_spread_holds(clock: _Clock) -> None:
    """A tie at the narrowest spread holds the level for six windows, then measures below it again."""
    pool = _pool(desired=40)

    levels = await _levels(pool, clock, *_TO_HOLD, *[5.0] * pool.hold_cycles_at_peak)

    assert levels[len(_TO_HOLD) - 1 :] == [28] * pool.hold_cycles_at_peak + [25]


async def test_zero_hold_cycles_never_hold(clock: _Clock) -> None:
    """A subclass can switch the hold off, and ties then keep moving the level."""

    class NeverHolds(ThroughputAutoscaledPool):
        hold_cycles_at_peak = 0

    pool = _pool(cls=NeverHolds, desired=40)

    levels = await _levels(pool, clock, *_TO_HOLD, 5.0, 5.0, 5.0)

    assert all(level != following for level, following in pairwise(levels))


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
async def test_drift_ends_the_hold(clock: _Clock, rates: list[float], expected: int) -> None:
    """Two held windows past the margin on one side of the first, or one at half of it, end the hold at 28."""
    pool = _pool(desired=40)
    await _levels(pool, clock, *_TO_HOLD, 5.0)

    assert (await _levels(pool, clock, *rates))[-1] == expected


async def test_unserved_first_held_window_ends_the_hold(clock: _Clock) -> None:
    """A first held window that delivers nothing before patience runs out ends the hold at once."""
    pool = _pool(desired=40)
    await _levels(pool, clock, *_TO_HOLD)

    assert await _stalled_window(pool, clock) == 25


async def test_held_window_can_lose_early(clock: _Clock) -> None:
    """A held window is given up once its bound falls below the first held window's rate by the margin."""
    pool = _pool(desired=40)
    await _levels(pool, clock, *_TO_HOLD, 5.0)

    assert pool._losing_bound(_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40), now=0.0) is not None
    assert pool._losing_bound(_cohort([0.4, 0.4], running=[0.4, 0.4], concurrency=40), now=0.0) is None


async def test_win_at_the_ceiling_holds_there(clock: _Clock) -> None:
    """An upper win at the concurrency limit holds the pool there for three windows, then looks below at 100 / 1.15."""
    pool = _pool(desired=90, max_concurrency=100)
    await _levels(pool, clock, 10.0, 11.0, 11.0, 5.0)

    assert await _levels(pool, clock, 20.0, 20.0, 20.0, 20.0) == [100, 100, 100, 87]


@pytest.mark.parametrize(
    ('cohort', 'finished'),
    [
        pytest.param(_cohort([1.0] * 8, running=[0.5, 0.5]), True, id='four fifths'),
        pytest.param(_cohort([1.0] * 7, running=[0.5] * 3), False, id='less'),
        pytest.param(_cohort(), False, id='empty'),
    ],
)
def test_window_finishes_at_four_fifths(cohort: _Cohort, finished: bool) -> None:  # noqa: FBT001
    """A window is read once four fifths of its members have finished, and never while empty."""
    assert _pool()._finished(cohort) is finished


def test_rate_is_littles_law() -> None:
    """The rate is concurrency times finished members over the time they occupied."""
    assert ThroughputAutoscaledPool._rate(_cohort([2.0] * 4, concurrency=8)) == pytest.approx(4.0)


async def test_upper_level_can_lose_early(clock: _Clock) -> None:
    """An upper level is given up once its bound falls below the lower level's rate by the margin."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)

    assert pool._losing_bound(_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40), now=0.0) is not None
    assert pool._losing_bound(_cohort([1.0, 1.0], running=[1.0, 1.0], concurrency=40), now=0.0) is None


async def test_slow_tail_does_not_lose_early(clock: _Clock) -> None:
    """A slow tail left running does not lose an upper level whose reading, taken without it, would win."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)

    # Eight of ten members make a reading: one more finishing at 30 seconds reads 40 * 8 / 37, above the lower level.
    assert pool._losing_bound(_cohort([1.0] * 7, running=[30.0] * 3, concurrency=40), now=0.0) is None


async def test_young_members_past_the_reading_raise_the_bound(clock: _Clock) -> None:
    """Running members past the ones a reading needs count toward the bound when they would raise the rate."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)

    # Eight members read 40 * 8 / 71, below the lower level, but all ten read 40 * 10 / 73, above it.
    assert pool._losing_bound(_cohort([10.0] * 7, running=[1.0] * 3, concurrency=40), now=0.0) is None


async def test_climb_step_can_lose_early(clock: _Clock) -> None:
    """A climb step is given up once its bound falls below the last step's rate by the climb margin."""
    pool = _pool(desired=10)
    await _levels(pool, clock, 10.0)

    assert pool._losing_bound(_cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=20), now=0.0) is not None
    assert pool._losing_bound(_cohort([0.5, 0.5], running=[0.5, 0.5], concurrency=20), now=0.0) is None


def test_finished_window_is_read() -> None:
    """A mostly finished window is read as its rate and sends the pool back to settling."""
    pool = _pool(desired=40)
    window = _cohort([1.0] * 8, running=[0.5, 0.5], concurrency=40)

    with patch.object(pool, '_window', window), patch.object(pool, '_read') as read:
        pool._wait(50.0)

    assert read.call_args.args[0] == pytest.approx(40.0)
    assert pool._stage_since == 50.0


async def test_losing_window_is_read_early(clock: _Clock) -> None:
    """A window whose bound already loses is read before it finishes, at that bound."""
    pool = _pool(desired=40)
    await _levels(pool, clock, 10.0, 10.0, 5.0)
    window = _cohort([1.0, 1.0], running=[30.0, 30.0], concurrency=40)

    # The running members' ages are counted from time zero.
    with patch.object(pool, '_window', window), patch.object(pool, '_read') as read:
        pool._wait(0.0)

    # Three of the four members make a reading, so the best case is one running member finishing at its age.
    assert read.call_args.args[0] == pytest.approx(40 * 3 / 32)


def test_window_out_of_patience_reads_zero() -> None:
    """A window that outlasts its patience reads as a level the target cannot serve."""
    pool = _pool(desired=40)
    window = _cohort([1.0], running=[0.0] * 9, concurrency=40)

    with (
        patch.object(ThroughputAutoscaledPool, '_patience_unit', new_callable=PropertyMock, return_value=10.0),
        patch.object(pool, '_window', window),
        patch.object(pool, '_stage_since', 0.0),
        patch.object(pool, '_read') as read,
    ):
        pool._wait(51.0)

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
    pool = _pool(desired=40)
    window = _cohort([1.0] * 10, concurrency=window_concurrency)

    with _live(live), patch.object(pool, '_window', window), patch.object(pool, '_read'):
        pool._wait(0.0)

    assert pool._filled is filled


async def test_reading_error_does_not_stall(clock: _Clock) -> None:
    """An error while reading a window still leaves the pool able to measure the next one."""
    pool = _pool(desired=40)
    window = _cohort([1.0] * 10, concurrency=40)

    with (
        patch.object(pool, '_window', window),
        patch.object(pool, '_read', side_effect=RuntimeError),
        pytest.raises(RuntimeError),
    ):
        pool._wait(clock.now)

    assert await _levels(pool, clock, 10.0) == [80]


@pytest.mark.parametrize(
    ('durations', 'expected'),
    [
        pytest.param([], float('inf'), id='nothing finished'),
        pytest.param([30.0], 30.0, id='recent duration'),
    ],
)
def test_first_window_patience(durations: list[float], expected: float) -> None:
    """Before any level is read, patience follows the recent task duration once there is one."""
    pool = _pool(desired=40)

    with patch.object(pool, '_durations', deque(durations)):
        assert pool._patience_unit == expected


async def test_overload_cuts_and_restarts(clock: _Clock) -> None:
    """An overloaded machine cuts by the parent's step and ends the climb, so a paying window no longer doubles."""
    status = _status()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    pool = _pool(desired=80, status=status)

    pool._autoscale()
    assert pool.desired_concurrency == 76

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    [level] = await _levels(pool, clock, 100.0)
    assert 76 <= level < 152


async def test_overload_ends_a_peak_hold(clock: _Clock) -> None:
    """An overload during a hold at the peak ends the hold, so the pool measures the levels around the cut again."""
    status = _status()
    pool = _pool(desired=40, status=status)
    await _levels(pool, clock, *_TO_HOLD)

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    pool._autoscale()
    assert pool.desired_concurrency == 26

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    assert await _levels(pool, clock, 5.0, 5.0) == [32, 30]


async def test_overload_forgets_the_last_win(clock: _Clock) -> None:
    """An upper win before an overload does not count toward resuming the climb after it, so 54 winning moves to 47."""
    status = _status()
    pool = _pool(desired=40, status=status)
    await _levels(pool, clock, 10.0, 10.0, 5.0, 130.0)

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    pool._autoscale()

    status.get_historical_system_info.return_value = MagicMock(is_system_idle=True)
    assert await _levels(pool, clock, 5.0, 100.0) == [54, 47]


@pytest.mark.parametrize(
    ('desired', 'expected'),
    [
        pytest.param(1, 1, id='floor'),
        pytest.param(3, 2, id='three'),
        pytest.param(4, 3, id='four'),
    ],
)
def test_overload_near_the_floor(desired: int, expected: int) -> None:
    """An overload near the minimum concurrency cuts by one parent step, and at the minimum leaves the pool there."""
    status = _status()
    status.get_historical_system_info.return_value = MagicMock(is_system_idle=False)
    pool = _pool(desired=desired, status=status)

    pool._autoscale()

    assert pool.desired_concurrency == expected


async def test_overload_after_a_raise_undoes_it(clock: _Clock) -> None:
    """An overload before the raised level was read returns the pool to the level before the raise."""
    status = _status()
    pool = _pool(desired=40, status=status)
    assert await _levels(pool, clock, 10.0) == [80]

    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    pool._autoscale()

    assert pool.desired_concurrency == 40


async def test_read_window_confirms_a_raise(clock: _Clock) -> None:
    """Once a window at the raised level has been read, an overload no longer undoes the raise."""
    status = _status()
    pool = _pool(desired=90, max_concurrency=100, status=status)
    assert await _levels(pool, clock, 10.0, 11.0, 11.0, 5.0, 20.0) == [100, 100, 60, 100, 100]

    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    pool._autoscale()

    assert pool.desired_concurrency == 100


def test_overload_now_without_a_raise_keeps_the_level() -> None:
    """A current overload with no raise to undo is left to the historical signal."""
    status = _status()
    status.get_current_system_info.return_value = MagicMock(is_system_idle=False)
    pool = _pool(desired=40, status=status)

    pool._autoscale()

    assert pool.desired_concurrency == 40


def test_soft_ceiling_lifts_when_idle() -> None:
    """The cap left by an overload lifts by the parent's step each idle tick."""
    status = _status()
    status.get_historical_system_info.side_effect = [MagicMock(is_system_idle=value) for value in (False, True)]
    pool = _pool(desired=80, status=status)

    pool._autoscale()
    pool._autoscale()

    assert pool._soft_ceiling == 80


def test_empty_window_reopens(clock: _Clock) -> None:
    """A window nobody joined is opened again rather than read."""
    pool = _pool(desired=40)

    with _live(40):
        clock.now = 100.0
        pool._autoscale()
        first = pool._window

        clock.now = 141.0
        pool._autoscale()

    assert pool._stage == 'enrol'
    assert pool._window is not first
    assert pool._window is not None
    assert pool._window.size == 0


async def test_window_closes_after_four_spans(clock: _Clock) -> None:
    """A window with too few members still closes after four spans."""
    pool = _pool(desired=40)

    with _live(40):
        clock.now = 100.0
        pool._autoscale()

        async with _members(pool, 3):
            clock.now = 141.0
            pool._autoscale()

            assert pool._stage == 'wait'


async def test_run_counts_settle_from_the_start(clock: _Clock) -> None:
    """Time between building the pool and running it does not count toward settling."""
    pool = _pool(desired=40)
    clock.now = 500.0

    with patch.object(AutoscaledPool, 'run', new=AsyncMock()):
        await pool.run()

    assert pool._stage_since == 500.0


async def test_run_drops_a_window_left_open(clock: _Clock) -> None:
    """A window a previous run left enrolling is not read in the next run."""
    pool = _pool(desired=40)

    with _live(40):
        clock.now = 100.0
        pool._autoscale()

    assert pool._stage == 'enrol'

    with patch.object(AutoscaledPool, 'run', new=AsyncMock()):
        await pool.run()

    assert (pool._stage, pool._window, pool._enrolling) == ('settle', None, None)


async def test_worker_records_its_duration(clock: _Clock) -> None:
    """A finished task joins its window and the recent durations."""
    pool = _pool(desired=40)

    with _live(40):
        clock.now = 100.0
        pool._autoscale()

    window = pool._window

    async with _members(pool, 1) as members:
        clock.advance(3.0)
        await members.finish(1)

    assert window is not None
    assert (window.size, len(window.finished), window.finished_total) == (1, 1, 3.0)
    assert list(pool._durations) == [3.0]


async def test_quick_task_has_a_rate(clock: _Clock) -> None:
    """A task too quick for the clock to time still gives its window a rate."""
    pool = _pool(desired=40)

    with _live(40):
        clock.now = 100.0
        pool._autoscale()

    window = pool._window

    with _live(40):
        async with _members(pool, 1):
            pass

    assert window is not None
    assert ThroughputAutoscaledPool._rate(window) > 0


async def test_settles_near_the_peak(clock: _Clock) -> None:
    """Against a target that slows past its capacity, the pool settles near the peak and short of its limit."""
    capacity = 40
    pool = _pool(desired=10, max_concurrency=100)
    peak = max(range(1, 101), key=lambda level: level / _duration(level, capacity))

    tail = _tail(await _run(pool, clock, capacity=capacity, minutes=30), minutes=5)

    assert pool.desired_concurrency < 100
    assert all(abs(level - peak) <= 0.25 * peak for level, _ in tail)
    delivered = sum(level for level, _ in tail) / sum(duration for _, duration in tail)
    assert delivered >= 0.9 * peak / _duration(peak, capacity)


async def test_holds_at_the_limit_below_the_knee(clock: _Clock) -> None:
    """Against a target that never slows below the limit, the pool climbs to the limit and stays there."""
    pool = _pool(desired=10, max_concurrency=100)

    tail = _tail(await _run(pool, clock, capacity=1000, minutes=30), minutes=5)

    levels = [level for level, _ in tail]
    assert levels.count(100) >= 0.75 * len(levels)


def test_emits_experimental_warning() -> None:
    """Building the pool warns that it is experimental."""
    with pytest.warns(UserWarning, match='experimental'):
        _pool()


async def test_tuning_by_subclassing(clock: _Clock) -> None:
    """Class attributes set on a subclass reach the pool."""

    class Wide(ThroughputAutoscaledPool):
        startup_gain = 3.0

    pool = _pool(cls=Wide, desired=20)

    assert await _levels(pool, clock, 10.0) == [60]
