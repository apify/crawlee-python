from __future__ import annotations

import asyncio
import math
from collections import deque
from contextlib import AsyncExitStack
from datetime import timedelta
from logging import WARNING, getLogger
from operator import attrgetter
from typing import TYPE_CHECKING

from cachetools import TTLCache, cachedmethod

from crawlee._types import ConcurrencySettings
from crawlee._utils.docs import docs_group
from crawlee._utils.log import LoggerOnce
from crawlee._utils.recurring_task import RecurringTask

from ._snapshotter import Snapshotter
from ._system_status import SystemStatus

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Self

    from crawlee.configuration import Configuration

    from ._types import ConcurrencyConsumer, SystemInfo

logger = getLogger(__name__)


def _validate_concurrency(value: int, name: str) -> int:
    """Check that a retuned concurrency bound is at least 1."""
    if value < 1:
        raise ValueError(f'{name} must be at least 1')

    return value


@docs_group('Autoscaling')
class ConcurrencySystem:
    """Decides whether there is free compute for one more task, and scales the concurrency budget over time.

    The system combines live system load, measured by an internal `Snapshotter`, with a concurrency budget that it
    scales up while the system is idle and down while it is overloaded.

    Sharing one instance between several crawlers caps their combined concurrency, instead of letting each of them
    scale independently and oversubscribe the machine. Whoever creates the instance owns its lifecycle: a crawler
    does that for the default system it builds, never for an injected one.

    ### Usage

    ```python
    import asyncio

    from crawlee import ConcurrencySettings
    from crawlee.autoscaling import ConcurrencySystem
    from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
    from crawlee.storages import RequestQueue


    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')


    async def main() -> None:
        settings = ConcurrencySettings(max_concurrency=20)

        # A queue each, so that the crawlers do not process each other's requests.
        first_queue = await RequestQueue.open(alias='first')
        second_queue = await RequestQueue.open(alias='second')

        async with ConcurrencySystem(settings) as concurrency_system:
            first = ParselCrawler(
                concurrency_system=concurrency_system,
                request_manager=first_queue,
                request_handler=handler,
            )
            second = ParselCrawler(
                concurrency_system=concurrency_system,
                request_manager=second_queue,
                request_handler=handler,
            )
            await asyncio.gather(first.run(['https://a.example']), second.run(['https://b.example']))


    asyncio.run(main())
    ```
    """

    _TASKS_PER_MINUTE_BUCKET = timedelta(seconds=1)
    """Time span one bucket of the per-minute task counter covers."""

    _TASKS_PER_MINUTE_WINDOW = timedelta(minutes=1)
    """Time span the per-minute task cap is counted over."""

    _TASKS_PER_MINUTE_BUCKETS = int(_TASKS_PER_MINUTE_WINDOW / _TASKS_PER_MINUTE_BUCKET)

    _CURRENT_LOAD_TTL = timedelta(milliseconds=50)
    """How long the live load verdict is reused, so that starting one task does not evaluate the load twice."""

    def __init__(
        self,
        concurrency_settings: ConcurrencySettings | None = None,
        *,
        desired_concurrency_ratio: float = 0.9,
        scale_up_step_ratio: float = 0.05,
        scale_down_step_ratio: float = 0.05,
        autoscale_interval: timedelta = timedelta(seconds=10),
        logging_interval: timedelta | None = timedelta(minutes=1),
        max_snapshot_age: timedelta = timedelta(seconds=5),
        configuration: Configuration | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            concurrency_settings: Settings of concurrency levels.
            desired_concurrency_ratio: Minimum ratio of the desired concurrency that has to be reached before the
                system scales up further.
            scale_up_step_ratio: Fraction of the desired concurrency added on each scale-up.
            scale_down_step_ratio: Fraction of the desired concurrency removed on each scale-down.
            autoscale_interval: Interval at which the desired concurrency is adjusted to the system load.
            logging_interval: Interval at which the system logs its state. `None` turns the logging off.
            max_snapshot_age: Age of the snapshots that decide whether the system is overloaded before one more task
                starts. Scaling looks at the whole 30-second snapshot history instead, so keep this shorter.
            configuration: The `Configuration` the internal `Snapshotter` is set up from. Uses the global one if not
                provided.
        """
        concurrency_settings = concurrency_settings or ConcurrencySettings()

        if autoscale_interval <= timedelta(0):
            raise ValueError('autoscale_interval must be positive')

        if logging_interval is not None and logging_interval <= timedelta(0):
            raise ValueError('logging_interval must be positive')

        # A non-positive age leaves no snapshots to judge the current load by, so nothing would ever look overloaded.
        if max_snapshot_age <= timedelta(0):
            raise ValueError('max_snapshot_age must be positive')

        if not 0 < desired_concurrency_ratio < 1:
            raise ValueError('desired_concurrency_ratio must be between 0 and 1, exclusive')

        if not 0 < scale_up_step_ratio < 1:
            raise ValueError('scale_up_step_ratio must be between 0 and 1, exclusive')

        if not 0 < scale_down_step_ratio < 1:
            raise ValueError('scale_down_step_ratio must be between 0 and 1, exclusive')

        self._min_concurrency = concurrency_settings.min_concurrency
        self._max_concurrency = concurrency_settings.max_concurrency
        self._desired_concurrency = concurrency_settings.desired_concurrency
        self._max_tasks_per_minute = concurrency_settings.max_tasks_per_minute
        self._desired_concurrency_ratio = desired_concurrency_ratio
        self._scale_up_step_ratio = scale_up_step_ratio
        self._scale_down_step_ratio = scale_down_step_ratio
        self._autoscale_interval = autoscale_interval
        self._current_concurrency = 0

        self._snapshotter = Snapshotter.from_config(configuration)
        self._system_status = SystemStatus(self._snapshotter, max_snapshot_age=max_snapshot_age)

        self._current_load: TTLCache[tuple, SystemInfo] = TTLCache(
            maxsize=1,
            ttl=self._CURRENT_LOAD_TTL.total_seconds(),
        )

        self._tasks_started = deque([0] * self._TASKS_PER_MINUTE_BUCKETS, maxlen=self._TASKS_PER_MINUTE_BUCKETS)
        self._age_tasks_started_task = RecurringTask(self._age_tasks_started, self._TASKS_PER_MINUTE_BUCKET)
        self._autoscale_task = RecurringTask(self._autoscale, autoscale_interval)
        self._log_system_status_task = (
            RecurringTask(self._log_system_status, logging_interval) if logging_interval is not None else None
        )
        self._recurring_tasks = [
            task
            for task in (self._autoscale_task, self._age_tasks_started_task, self._log_system_status_task)
            if task is not None
        ]

        self._logger_once = LoggerOnce(logger)
        self._measurement = AsyncExitStack()
        self._active = False

    @property
    def min_concurrency(self) -> int:
        """The minimum number of tasks running in parallel, kept up even while the machine is overloaded.

        Can be retuned while the system runs. Setting it too high for the available memory and CPU can make the crawl
        run very slowly or crash.
        """
        return self._min_concurrency

    @min_concurrency.setter
    def min_concurrency(self, value: int) -> None:
        self._min_concurrency = _validate_concurrency(value, 'min_concurrency')
        self._clamp_desired_concurrency()

    @property
    def max_concurrency(self) -> int:
        """The maximum number of tasks running in parallel. Can be retuned while the system runs."""
        return self._max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value: int) -> None:
        self._max_concurrency = _validate_concurrency(value, 'max_concurrency')
        self._clamp_desired_concurrency()

    @property
    def desired_concurrency(self) -> int:
        """The number of tasks that should currently be running in parallel, given a large enough supply of them.

        Read-only: the value belongs to the autoscaling, which recomputes it from the load. Retune the budget through
        `min_concurrency` and `max_concurrency` instead.
        """
        return self._desired_concurrency

    @property
    def current_concurrency(self) -> int:
        """The number of parallel tasks currently booked against this system, whichever pool booked them."""
        return self._current_concurrency

    @property
    def active(self) -> bool:
        """Whether the system is monitoring the load and scaling the budget."""
        return self._active

    async def __aenter__(self) -> Self:
        """Start monitoring the system load and scaling the budget.

        Raises:
            RuntimeError: If the system is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        # The window does not age while the system is down, so the starts of a previous session must not count.
        self._tasks_started = deque([0] * self._TASKS_PER_MINUTE_BUCKETS, maxlen=self._TASKS_PER_MINUTE_BUCKETS)

        try:
            await self._measurement.enter_async_context(self._snapshotter)

            self._autoscale_task.start()

            if self._log_system_status_task is not None:
                self._log_system_status_task.start()

            # Only the capped system has to age its buckets; an uncapped one never reads them.
            if self._max_tasks_per_minute != float('inf'):
                self._age_tasks_started_task.start()
        except BaseException:
            # A half-entered system is nobody's to exit, as `__aexit__` refuses to run while the system is inactive,
            # so whatever did get started is torn down here. Stopping a task that never started is a no-op.
            await asyncio.gather(*(task.stop() for task in self._recurring_tasks), return_exceptions=True)
            await self._measurement.aclose()
            raise

        # Last, so that `active` never claims a system whose snapshots are not being collected yet.
        self._active = True

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Stop monitoring the system load and scaling the budget.

        Raises:
            RuntimeError: If the system is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        # First, so that a failing shutdown cannot leave a dead system claiming to be active.
        self._active = False

        # Tear everything down even if a stop fails, for example on a cancellation, since an inactive system cannot be
        # exited again.
        try:
            for result in await asyncio.gather(
                *(task.stop() for task in self._recurring_tasks), return_exceptions=True
            ):
                if isinstance(result, BaseException):
                    raise result
        finally:
            await self._measurement.__aexit__(exc_type, exc_value, exc_traceback)

    def has_capacity_for_task(self, consumer: ConcurrencyConsumer | None = None) -> bool:  # noqa: ARG002
        """Report whether one more task may start right now.

        One budget serves the whole machine, so the consumer asking makes no difference here, and may be left out
        when the answer is queried directly rather than through a pool.

        Args:
            consumer: The consumer asking for capacity.

        Returns:
            `True` if a task may start, `False` if the budget is spent or the machine is overloaded.

        Raises:
            RuntimeError: If the system is not active, as nothing measures the load or scales the budget then.
        """
        self._raise_if_inactive()

        if self._current_concurrency >= self._desired_concurrency:
            logger.debug('Not scheduling new tasks - already running at desired concurrency')
            return False

        is_system_idle = self._get_current_load().is_system_idle

        # Below the minimum the pool keeps going regardless, so that an overloaded machine cannot stall a crawl.
        if not is_system_idle and self._current_concurrency >= self._min_concurrency:
            logger.debug('Not scheduling new tasks - system is overloaded')
            return False

        return True

    def get_current_status(self) -> SystemInfo:
        """Report what the system currently makes of the machine, on the window that gates task dispatch.

        Useful for answering why a crawl does not scale up.

        Raises:
            RuntimeError: If the system is not active, as nothing is sampling the load then.
        """
        self._raise_if_inactive()
        return self._system_status.get_current_system_info()

    def try_register_task_start(self, consumer: ConcurrencyConsumer, task_id: str) -> bool:  # noqa: ARG002
        """Book a task against the budget, or report that there is no room for it.

        The per-minute cap is enforced here rather than in `has_capacity_for_task`, so that an empty queue is never
        blocked for a whole minute.

        Args:
            consumer: The consumer the task is booked for.
            task_id: The id the task runs under, repeated by `register_task_end`. Unused here.

        Returns:
            `True` if the task was booked, `False` if there was no room for it.

        Raises:
            RuntimeError: If the system is not active.
        """
        if not self.has_capacity_for_task(consumer):
            return False

        if self._is_over_max_tasks_per_minute:
            logger.debug('Not scheduling new tasks - maximum tasks per minute reached')
            return False

        self._current_concurrency += 1
        self._tasks_started[-1] += 1
        return True

    def register_task_end(self, consumer: ConcurrencyConsumer, task_id: str) -> None:  # noqa: ARG002
        """Return a task's slot to the budget.

        Args:
            consumer: The consumer the task was booked for.
            task_id: The id the task was booked under. Unused here.
        """
        if self._current_concurrency == 0:
            self._logger_once.log(
                'Ignoring the end of a task that never started. Nothing is running right now, so something is '
                'ending more tasks than it started.',
                key='concurrency_system_unbooked_task_end',
                level=WARNING,
            )
            return

        self._current_concurrency -= 1

    @cachedmethod(attrgetter('_current_load'))
    def _get_current_load(self) -> SystemInfo:
        """Read the live load verdict, cached for `_CURRENT_LOAD_TTL`."""
        return self._system_status.get_current_system_info()

    def _raise_if_inactive(self) -> None:
        if not self._active:
            raise RuntimeError(
                f'The {self.__class__.__name__} is not active, so nothing is measuring the system load and the '
                f'concurrency is no longer being adjusted. Whoever creates the system owns its lifecycle: keep it '
                f'open with `async with` until every pool and crawler borrowing it has finished.'
            )

    @property
    def _is_over_max_tasks_per_minute(self) -> bool:
        """Whether the per-minute task cap has been reached."""
        if self._max_tasks_per_minute == float('inf'):
            return False

        return sum(self._tasks_started) >= self._max_tasks_per_minute

    def _clamp_desired_concurrency(self) -> None:
        """Keep the desired concurrency within the bounds, letting the maximum win over a minimum above it."""
        at_least_min = max(self._desired_concurrency, self._min_concurrency)
        self._desired_concurrency = min(at_least_min, self._max_concurrency)

    def _age_tasks_started(self) -> None:
        """Move the per-minute counter on by one bucket, dropping the oldest one."""
        self._tasks_started.append(0)

    def _autoscale(self) -> None:
        """Adjust the desired concurrency to the historical system status. Do not call directly."""
        status = self._system_status.get_historical_system_info()

        min_current_concurrency = math.floor(self._desired_concurrency_ratio * self._desired_concurrency)

        # A run held back by the per-minute cap cannot fill a larger budget, so growing it is pointless. Shrinking it
        # is not: an overloaded machine has to be relieved whether or not the run is capped.
        should_scale_up = (
            status.is_system_idle
            and not self._is_over_max_tasks_per_minute
            and self._desired_concurrency < self._max_concurrency
            and self._current_concurrency >= min_current_concurrency
        )
        should_scale_down = not status.is_system_idle and self._desired_concurrency > self._min_concurrency

        if should_scale_up:
            step = math.ceil(self._scale_up_step_ratio * self._desired_concurrency)
            self._desired_concurrency = min(self._max_concurrency, self._desired_concurrency + step)
            logger.debug(f'Scaled up the desired concurrency to {self._desired_concurrency}, {status}')
        elif should_scale_down:
            step = math.ceil(self._scale_down_step_ratio * self._desired_concurrency)
            self._desired_concurrency = max(self._min_concurrency, self._desired_concurrency - step)
            logger.debug(f'Scaled down the desired concurrency to {self._desired_concurrency}, {status}')

    def _log_system_status(self) -> None:
        system_status = self._system_status.get_historical_system_info()

        logger.debug(
            f'current concurrency {self._current_concurrency}, desired {self._desired_concurrency}, {system_status}'
        )
