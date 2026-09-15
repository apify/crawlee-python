from __future__ import annotations

import math
import statistics
import time
import warnings
from collections import deque
from logging import getLogger
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

from crawlee._autoscaling.autoscaled_pool import AutoscaledPool
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from crawlee._autoscaling.system_status import SystemStatus
    from crawlee._types import ConcurrencySettings

logger = getLogger(__name__)

_DURATION_WINDOW = 50
"""Recent tasks the task duration is read from, as a median."""

_MAX_SETTLE_TICKS = 4
"""Most scaling ticks the pool waits for the target to meet a new level before measuring it."""

_MAX_ENROL_SPANS = 4
"""Most window spans a window keeps enrolling for while it has too few members."""

_CHANGE_WINDOWS = 2
"""Consecutive held windows past `hold_change_margin` on one side that end a hold."""

_DECISIVE_DROP = 0.5
"""Fraction a held window's rate falls by that ends a hold on its own."""

_MIN_DURATION = 1e-6
"""Seconds a task is recorded as at least, for clocks too coarse to time a quick one."""

_Phase = Literal['centre', 'climb', 'high', 'low']
_Stage = Literal['settle', 'enrol', 'wait']


class _Cohort:
    """The tasks that started while one window was enrolling, and what has become of them."""

    def __init__(self) -> None:
        self.size = 0
        self.concurrency_sum = 0
        self.running = dict[int, float]()
        self.finished = list[float]()
        self.finished_total = 0.0

    def join(self, started_at: float, concurrency: int) -> int:
        """Enrol a task that started at `started_at` among `concurrency` running tasks, and return its member number."""
        self.size += 1
        self.running[self.size] = started_at
        self.concurrency_sum += concurrency
        return self.size

    def leave(self, member: int, elapsed: float) -> None:
        """Record that a member finished after `elapsed` seconds."""
        del self.running[member]
        self.finished.append(elapsed)
        self.finished_total += elapsed

    @property
    def concurrency(self) -> float:
        """Mean concurrency the members met when they started."""
        return self.concurrency_sum / self.size if self.size else 0.0

    def median_ceiling(self) -> float | None:
        """Get the finished members' duration at the median's rank, which the median can only fall below."""
        rank = self.size // 2 + 1
        return sorted(self.finished)[rank - 1] if len(self.finished) >= rank else None

    def median(self, now: float) -> float | None:
        """Get the median duration once no member still running can change it, else `None`."""
        if (upper := self.median_ceiling()) is None:
            return None

        if self.running and now - max(self.running.values()) < upper:
            return None

        return upper if self.size % 2 else math.sqrt(sorted(self.finished)[self.size // 2 - 1] * upper)


@docs_group('Autoscaling')
class ThroughputAutoscaledPool(AutoscaledPool):
    """An `AutoscaledPool` that settles near the concurrency at which its targets deliver the most.

    `AutoscaledPool` climbs until a machine resource runs out, so against a target that stops answering faster under
    load it keeps adding concurrency, which costs throughput and strains the target. This pool measures how many tasks
    per second two levels around a centre deliver and moves the centre toward the better one. It climbs by doublings
    at the start and holds for a while once the two levels tie. Every machine-resource check of the parent still
    applies.

    Each step waits for its tasks to finish, so on slow pages the pool reaches a high concurrency later than
    `AutoscaledPool` does. Every completion counts as delivered work, so a target that answers faster by refusing
    requests reads as one that can take more.

    Tune it by subclassing and overriding the class attributes below.

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.

    ### Usage

    ```python
    from crawlee import ThroughputAutoscaledPool
    from crawlee.crawlers import ParselCrawler

    crawler = ParselCrawler(autoscaled_pool_class=ThroughputAutoscaledPool)
    ```
    """

    dither_min = 0.15
    """Fraction the two levels sit above and below the centre once it has converged."""

    dither_max = 0.5
    """Largest fraction the two levels spread to while the centre keeps moving one way."""

    margin = 0.10
    """Fraction one level's rate must exceed the other's by before the centre moves toward it."""

    startup_gain = 2.0
    """Factor the concurrency grows by per step of the opening climb."""

    climb_margin = 0.25
    """Fraction a climb step must deliver over the level before for the climb to go on."""

    retry_latency_margin = 0.5
    """Fraction a climb step's median latency may rise by and still be measured again when it delivered no more."""

    reentry_gain = 1.5
    """Factor the climb resumes by when the upper level keeps winning by the climb margin."""

    finished_fraction = 0.8
    """Share of a window's members that must finish before its rate is read."""

    min_members = 10
    """Members a window enrols before it closes."""

    patience_dead_times = 5.0
    """Task durations a window may take to finish before it counts as a level the target cannot serve."""

    hold_cycles_at_peak = 6
    """Windows the pool holds its centre for once the two levels tie at the narrowest spread. Zero never holds."""

    hold_change_margin = 0.2
    """Fraction a held window's rate may drift from the first held window's before the hold ends."""

    hold_cycles_at_ceiling = 3
    """Windows the pool holds at its concurrency limit after the limit won, before it looks below again."""

    def __init__(
        self,
        *,
        system_status: SystemStatus,
        concurrency_settings: ConcurrencySettings | None = None,
        run_task_function: Callable[[], Awaitable],
        is_task_ready_function: Callable[[], Awaitable[bool]],
        is_finished_function: Callable[[], Awaitable[bool]],
    ) -> None:
        """Initialize a new instance.

        Args:
            system_status: Provides data about system utilization (load).
            concurrency_settings: Settings of concurrency levels.
            run_task_function: A function that performs an asynchronous resource-intensive task.
            is_task_ready_function: A function that indicates whether `run_task_function` should be called.
            is_finished_function: A function that is called only when there are no tasks to be processed. If it
                resolves to `True` then the pool's run finishes.
        """
        super().__init__(
            system_status=system_status,
            concurrency_settings=concurrency_settings,
            run_task_function=run_task_function,
            is_task_ready_function=is_task_ready_function,
            is_finished_function=is_finished_function,
        )
        warnings.warn(
            'The `ThroughputAutoscaledPool` is experimental and may change or be removed in future releases.',
            category=UserWarning,
            stacklevel=2,
        )

        self._durations = deque[float](maxlen=_DURATION_WINDOW)
        self._enrolling: _Cohort | None = None
        self._window: _Cohort | None = None

        self._centre = float(self._desired_concurrency)
        self._dither = self.dither_max
        self._phase: _Phase = 'centre'
        self._stage: _Stage = 'settle'
        self._stage_since = time.monotonic()
        self._dead_time_at_move = 0.0
        self._reference_duration = 0.0
        self._soft_ceiling = self._max_concurrency
        # The level before the last raise, kept until a window at the raised level has been read.
        self._level_before_raise: int | None = None
        self._filled = True
        self._rates = dict[str, float]()
        self._last_direction = 0

        # The opening climb, and a reading from its last step kept for the first comparison after it.
        self._startup = True
        self._climb_gain = self.startup_gain
        self._climb_retried = False
        self._reference: float | None = None
        self._reference_median: float | None = None
        self._reused = False

        # A hold at the ceiling or at a peak, and what a peak hold compares its windows with.
        self._hold_cycles = 0
        self._holding_peak = False
        self._hold_reference: float | None = None
        self._hold_streak = 0

        self._set_level()

    @override
    async def run(self) -> None:
        """Start the pool, settling its level from now and dropping any window a previous run left open."""
        self._stop_enrolling()
        self._window = None
        self._stage, self._stage_since = 'settle', time.monotonic()
        await super().run()

    @override
    async def _worker_task(self) -> None:
        started_at = time.monotonic()
        cohort = self._enrolling
        member = cohort.join(started_at, self.current_concurrency) if cohort is not None else None

        try:
            await super()._worker_task()
        finally:
            elapsed = max(time.monotonic() - started_at, _MIN_DURATION)
            if cohort is not None and member is not None:
                cohort.leave(member, elapsed)
            self._durations.append(elapsed)

    @property
    def _tick(self) -> float:
        return self._AUTOSCALE_INTERVAL.total_seconds()

    @property
    def _dead_time(self) -> float | None:
        return statistics.median(self._durations) if self._durations else None

    @property
    def _window_span(self) -> float:
        """Seconds a window enrols the tasks that start during it: one task duration, never less than a tick."""
        return max(self._tick, self._dead_time or 0.0)

    @property
    def _patience_unit(self) -> float:
        """Seconds patience is counted in: the median task duration of the last level read, else the recent one."""
        if math.isinf(self._dead_time_at_move) and self._dead_time is not None:
            return max(self._dead_time, self._tick)
        return self._dead_time_at_move

    def _open_window(self) -> None:
        self._window = self._enrolling = _Cohort()

    def _stop_enrolling(self) -> None:
        self._enrolling = None

    def _ceiling(self) -> int:
        return max(self._min_concurrency, min(self._max_concurrency, self._soft_ceiling))

    def _level(self, phase: _Phase) -> int:
        ceiling = self._ceiling()
        if self._hold_cycles > 0:
            return max(self._min_concurrency, min(ceiling, round(self._centre))) if self._holding_peak else ceiling

        if phase == 'centre':
            factor = 1.0
        elif phase == 'climb':
            factor = self._climb_gain
        elif phase == 'high':
            factor = 1 + self._dither
        else:
            factor = 1 / (1 + self._dither)

        level = round(self._centre * factor)
        # Near the floor both levels can round to the centre, and two windows at one level can only ever tie.
        if phase == 'high':
            level = max(level, round(self._centre) + 1)
        elif phase == 'low':
            level = min(level, round(self._centre) - 1)

        return max(self._min_concurrency, min(ceiling, level))

    def _set_level(self) -> None:
        level = self._level(self._phase)
        if level > self._desired_concurrency:
            self._level_before_raise = self._desired_concurrency
        elif level < self._desired_concurrency:
            self._level_before_raise = None
        self._desired_concurrency = level
        # Past the knee recent durations are the congested ones, so patience comes from the last level that finished.
        fallback = self._dead_time if self._dead_time is not None else float('inf')
        self._dead_time_at_move = max(self._reference_duration or fallback, self._tick)

    @staticmethod
    def _rate(cohort: _Cohort) -> float:
        """Get tasks per second by Little's law, over the members that have finished."""
        return cohort.concurrency * len(cohort.finished) / cohort.finished_total

    def _required(self, cohort: _Cohort) -> int:
        return max(1, math.floor(self.finished_fraction * cohort.size))

    def _finished(self, cohort: _Cohort) -> bool:
        return len(cohort.finished) >= self._required(cohort)

    def _losing_bound(self, cohort: _Cohort, now: float) -> float | None:
        """Get the best rate the window can still be read at, if even that already loses its comparison."""
        if self._hold_reference is not None:
            other, margin = self._hold_reference, self.hold_change_margin
        elif self._phase == 'climb':
            other, margin = self._reference, self.climb_margin
        elif self._phase == 'high':
            other, margin = self._rates.get('low'), self.margin
        else:
            return None

        if other is None or not cohort.size:
            return None

        # The members still needed for a reading, and any more that would raise the rate, finish now at their ages,
        # youngest first. The rate is read over finished members only, so a slow tail left running cannot lower it.
        required = self._required(cohort)
        count, occupied = len(cohort.finished), cohort.finished_total
        bound = 0.0
        for age in sorted(now - started for started in cohort.running.values()):
            count += 1
            occupied += age
            if count >= required:
                bound = max(bound, cohort.concurrency * count / occupied if occupied > 0 else float('inf'))

        return bound if bound < other / (1 + margin) else None

    def _lift_soft_ceiling(self) -> None:
        if self._soft_ceiling < self._max_concurrency:
            lifted = math.ceil(self._soft_ceiling * (1 + self._SCALE_UP_STEP_RATIO))
            self._soft_ceiling = min(self._max_concurrency, lifted)

    @override
    def _autoscale(self) -> None:
        now = time.monotonic()

        # A raise the machine objects to at once failed as a step, before the slower historical signal can tell.
        if self._level_before_raise is not None and not self._system_status.get_current_system_info().is_system_idle:
            self._on_overload(now, cut_to=self._level_before_raise)
            return

        if not self._system_status.get_historical_system_info().is_system_idle:
            self._on_overload(now)
            return

        self._lift_soft_ceiling()

        if self._stage == 'settle':
            self._settle(now)
        elif self._stage == 'enrol':
            self._enrol(now)
        else:
            self._wait(now)

    def _on_overload(self, now: float, *, cut_to: int | None = None) -> None:
        """Cut to `cut_to` or by the parent's step, cap the pool at the cut, and restart the cycle there."""
        self._stop_enrolling()
        self._window = None
        self._level_before_raise = None
        if cut_to is not None:
            self._desired_concurrency = cut_to
        elif self._desired_concurrency > self._min_concurrency:
            step = math.ceil(self._SCALE_DOWN_STEP_RATIO * self._desired_concurrency)
            self._desired_concurrency = max(self._min_concurrency, self._desired_concurrency - step)

        reason = 'right after a raise, undoing it' if cut_to is not None else 'cutting by the parent step'
        logger.debug(f'Machine overloaded {reason}; desired concurrency is now {self._desired_concurrency}')

        # The cut level becomes the lower level of the next cycle, so a burst is cut at the parent's pace. Near the
        # floor the centre sits a task above it, since the lower level is always at least a task below the centre.
        self._soft_ceiling = self._desired_concurrency
        self._centre = max(self._desired_concurrency * (1 + self.dither_min), self._desired_concurrency + 1.0)
        self._dither = self.dither_min
        self._end_hold()
        self._rates.clear()
        self._reused = False
        self._last_direction = 0
        self._startup = False
        self._phase, self._stage, self._stage_since = 'low', 'settle', now
        self._set_level()

    def _settle(self, now: float) -> None:
        """Wait for the target to meet the level, then open a window on it."""
        # The climb only waits for the pool to fill the level: a window enrolled during the fill reads its mean.
        filled = not self._startup or (
            self.current_concurrency >= self._DESIRED_CONCURRENCY_RATIO * self._desired_concurrency
        )
        settle = 0.0 if self._startup and filled else min(self._dead_time_at_move, _MAX_SETTLE_TICKS * self._tick)

        if now - self._stage_since < settle:
            return

        if not filled:
            # The request loader hands out less than this level takes, so nothing above it can be measured either.
            self._end_climb(0.0, measured=False)
            self._stage_since = now
            self._set_level()
            return

        self._open_window()
        self._stage, self._stage_since = 'enrol', now

    def _enrol(self, now: float) -> None:
        """Close the window once it has spanned a task duration with enough members."""
        elapsed = now - self._stage_since
        span = self._window_span
        size = self._window.size if self._window is not None else 0

        if elapsed >= _MAX_ENROL_SPANS * span and size == 0:
            # Nothing started because the tasks of a higher level still hold every slot. That is not a reading.
            self._open_window()
            self._stage_since = now
            return

        if elapsed >= span and (size >= self.min_members or elapsed >= _MAX_ENROL_SPANS * span):
            self._stop_enrolling()
            self._stage, self._stage_since = 'wait', now

    def _wait(self, now: float) -> None:
        """Read the window once enough of it has finished, or once it has already lost, or once patience runs out."""
        window = self._window
        if window is None:
            return

        median = window.median(now)
        if self._finished(window):
            rate = self._rate(window)
            self._reference_duration = median or self._reference_duration
        elif (bound := self._losing_bound(window, now)) is not None:
            rate = bound
        elif now - self._stage_since > self.patience_dead_times * self._patience_unit:
            rate = 0.0
        else:
            return

        # The stage moves on first, so an error while reading cannot leave the pool waiting on no window.
        self._window = None
        self._stage, self._stage_since = 'settle', now

        # A level counts as filled only if it was filled while the window enrolled and still is now.
        wanted = self._DESIRED_CONCURRENCY_RATIO * self._desired_concurrency
        self._filled = window.concurrency >= wanted and self.current_concurrency >= wanted

        self._read(rate, median if median is not None else window.median_ceiling())
        # The level this window measured has run without the machine objecting, so it no longer needs undoing.
        self._level_before_raise = None
        self._set_level()

    def _end_climb(self, rate: float, *, measured: bool) -> None:
        cause = 'a step stopped paying off' if measured else 'the level could not be filled'
        logger.debug(f'Climb ended at desired concurrency {self._desired_concurrency}: {cause}')
        self._startup = False
        self._phase = 'low'
        self._climb_gain = self.startup_gain
        if measured:
            # The level the climb lost at stands in for the upper level of the first cycle.
            self._rates['high'] = rate
            self._reused = True

    def _read(self, rate: float, median: float | None) -> None:
        if self._startup:
            self._read_climb(rate, median)
            return

        if self._hold_cycles > 0:
            self._read_hold(rate)
            return

        self._rates[self._phase] = rate
        if 'high' not in self._rates or 'low' not in self._rates:
            self._phase = 'low' if self._phase == 'high' else 'high'
            return

        self._compare(median)

    def _read_climb(self, rate: float, median: float | None) -> None:
        # A first level the target cannot serve is no base to double from.
        if not self._filled or (self._reference is None and rate <= 0):
            self._end_climb(rate, measured=False)
            return

        if self._reference is None or rate > self._reference * (1 + self.climb_margin):
            self._reference = rate
            self._reference_median = median
            if self._phase == 'climb':
                self._centre = float(self._desired_concurrency)
            self._phase = 'climb'
            self._climb_retried = False
            return

        # A step that delivered as much once is as likely a small window reading low as the knee, unless its latency
        # rose with it, which is the plateau past the knee.
        inflated = (
            median is not None
            and self._reference_median is not None
            and median > self._reference_median * (1 + self.retry_latency_margin)
        )
        if not self._climb_retried and rate >= self._reference and not inflated:
            self._climb_retried = True
            return

        self._end_climb(rate, measured=True)

    def _read_hold(self, rate: float) -> None:
        self._rates.clear()
        self._phase = 'low'

        if not self._holding_peak:
            self._hold_cycles -= 1
            # The soft ceiling may lift during the hold, and the centre follows it so the next cycle starts from there.
            self._centre = float(self._ceiling())
            return

        if self._hold_reference is None:
            if rate <= 0:
                # The target could not serve the held level at all, so there is nothing to hold.
                self._end_hold()
                return
            self._hold_reference = rate
        else:
            reference, margin = self._hold_reference, self.hold_change_margin
            # The same thresholds as in `_losing_bound`, so a window read early always counts as a drop.
            side = 1 if rate > reference * (1 + margin) else -1 if rate < reference / (1 + margin) else 0
            self._hold_streak = side if side * self._hold_streak <= 0 else self._hold_streak + side
            if rate < reference * (1 - _DECISIVE_DROP):
                self._hold_streak = -_CHANGE_WINDOWS
            if abs(self._hold_streak) >= _CHANGE_WINDOWS:
                self._hold_cycles = 1

        self._hold_cycles -= 1
        if self._hold_cycles == 0:
            self._end_hold()

    def _end_hold(self) -> None:
        if self._holding_peak or self._hold_cycles > 0:
            logger.debug(f'Hold ended; measuring around concurrency {round(self._centre)} again')
        self._hold_cycles = 0
        self._holding_peak = False
        self._hold_reference = None
        self._hold_streak = 0

    def _compare(self, median: float | None) -> None:
        high, low = self._rates['high'], self._rates['low']
        self._rates.clear()

        if high > low * (1 + self.margin) and self._filled:
            direction = 1
        elif low > high * (1 + self.margin):
            direction = -1
        else:
            direction = 0

        if self._reused and direction > 0:
            # The climb's reading is a phase old and from higher up, so it may lose or tie but never win.
            self._reused = False
            self._rates['low'] = low
            self._phase = 'high'
            return
        self._reused = False

        self._phase = 'low'
        if direction == 0:
            self._on_tie()
        else:
            self._on_win(direction, high, low, median)
        self._last_direction = direction

    def _on_tie(self) -> None:
        if self._dither <= self.dither_min and self.hold_cycles_at_peak > 0:
            # A step-down before the hold would have no way back up, so the hold sits between the two tied levels.
            logger.debug(f'Levels tied at the narrowest spread; holding concurrency {round(self._centre)}')
            self._hold_cycles = self.hold_cycles_at_peak
            self._holding_peak = True
            return

        # On a flat peak, or with an upper level the tasks could not fill, the lower concurrency delivers as much.
        self._centre = max(float(self._min_concurrency), self._centre / (1 + self._dither / 2))
        self._dither = max(self.dither_min, self._dither / 2)

    def _on_win(self, direction: int, high: float, low: float, median: float | None) -> None:
        """Move the centre toward the winning level, or resume the climb from it."""
        # The upper level was read last, so it is still the one set.
        clamped = direction > 0 and self._desired_concurrency >= self._ceiling()
        self._centre = float(self._desired_concurrency if direction > 0 else self._level('low'))

        if direction > 0 and not clamped and high > low * (1 + self.climb_margin) and self._last_direction > 0:
            # Twice running the upper level delivered what a doubling would, so the climb ended early on noise.
            self._startup = True
            self._climb_gain = self.reentry_gain
            self._climb_retried = False
            self._reference = high
            self._reference_median = median
            self._phase = 'climb'
            logger.debug(
                f'Upper level kept winning by a climb step; climbing again from concurrency {self._centre:.0f}'
            )
            return

        if clamped:
            logger.debug(f'Upper level won at the concurrency limit; holding at {self._desired_concurrency}')
            self._dither = self.dither_min
            self._hold_cycles = self.hold_cycles_at_ceiling
            self._holding_peak = False
        elif direction == self._last_direction:
            self._dither = min(self.dither_max, self._dither * 2)
        else:
            self._dither = max(self.dither_min, self._dither / 2)
