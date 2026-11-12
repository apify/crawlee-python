from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Generic

from typing_extensions import TypeVar

from crawlee._utils.log import LoggerOnce
from crawlee.autoscaling import AutoscaledPool
from crawlee.statistics import StatisticsState

if TYPE_CHECKING:
    import logging
    from collections.abc import Awaitable, Callable

    from crawlee.autoscaling import ConcurrencyConsumer
    from crawlee.autoscaling._types import ConcurrencySystemInterface

TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)


class CrawlerRun(Generic[TStatisticsState]):
    """Everything that belongs to a single `BasicCrawler.run` call.

    The crawler builds a fresh instance for every run, so nothing has to be reset between runs. A finished run stays
    readable as the history of the last crawl, so liveness is asked about explicitly with `is_live`.
    """

    _MIN_WAKE_DELAY = timedelta(milliseconds=10)
    """The shortest delay of a wake-up. A source that keeps reporting waiting past its `ready_at` would otherwise be
    re-checked in a busy loop.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger,
        concurrency_system: ConcurrencySystemInterface,
        consumer: ConcurrencyConsumer,
        run_task_function: Callable[[str], Awaitable],
        is_task_ready_function: Callable[[], Awaitable[bool]],
        is_finished_function: Callable[[], Awaitable[bool]],
    ) -> None:
        self._autoscaled_pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=consumer,
            run_task_function=run_task_function,
            is_task_ready_function=is_task_ready_function,
            is_finished_function=is_finished_function,
            logger=logger.getChild('autoscaled_pool'),
        )
        self._logger_once = LoggerOnce(logger)
        self._stop_requested = False
        self._failed = False
        self._dispatching = False
        self._finished = False
        self._wake_at: datetime | None = None
        self._wake_timer: asyncio.TimerHandle | None = None

        self.handled_request = False
        """Whether this run has marked any request as handled."""

        self.previous_state: TStatisticsState | None = None
        """The statistics state at the last status update of this run."""

    @property
    def is_live(self) -> bool:
        """Whether the `BasicCrawler.run` call this belongs to is still executing."""
        return not self._finished

    @property
    def is_dispatching(self) -> bool:
        """Whether the task loop is dispatching requests. Pausing a run before then would keep it from ever starting."""
        return self._dispatching

    @property
    def stop_requested(self) -> bool:
        """Whether a graceful shutdown of this run has been requested."""
        return self._stop_requested

    @property
    def failed(self) -> bool:
        """Whether a request has failed while the crawler aborts on an error."""
        return self._failed

    async def dispatch_requests(self) -> None:
        """Run the task loop until the crawl is over. The concurrency system has to be active by now."""
        self._dispatching = True

        try:
            await self._autoscaled_pool.run()
        finally:
            self._dispatching = False

    async def pause(self, *, timeout: timedelta | None = None) -> None:
        """Stop dispatching new requests and wait until the ones in progress finish.

        A stopped or failed run still ends.
        """
        try:
            await self._autoscaled_pool.pause(timeout=timeout)
        finally:
            self._release_if_ending()

    def resume(self) -> None:
        """Resume dispatching after `pause`."""
        self._autoscaled_pool.resume()

    def schedule_wake(self, ready_at: datetime) -> None:
        """Wake the task loop at `ready_at`, unless an earlier wake-up is already scheduled.

        The wake-up comes no sooner than `_MIN_WAKE_DELAY` from now.
        """
        if self._finished or (self._wake_at is not None and self._wake_at <= ready_at):
            return

        if self._wake_timer is not None:
            self._wake_timer.cancel()

        delay = max(self._MIN_WAKE_DELAY, ready_at - datetime.now(UTC)).total_seconds()
        self._wake_at = ready_at
        self._wake_timer = asyncio.get_running_loop().call_later(delay, self._wake)

    def _wake(self) -> None:
        self._wake_at = None
        self._wake_timer = None
        self._autoscaled_pool.notify()

    def stop(self) -> None:
        """Ask the run to stop taking new requests. A paused run resumes, so that it can notice the stop and finish."""
        self._stop_requested = True
        self._release_if_ending()

    def fail(self) -> None:
        """Mark the run as failed, so that it takes no new requests. A paused run resumes, so that it can finish."""
        self._failed = True
        self._release_if_ending()

    def _release_if_ending(self) -> None:
        """Lift a pause once the run is stopped or failed, since a paused pool never checks whether the run is over."""
        if self._stop_requested or self._failed:
            self._autoscaled_pool.resume()

    def log_once(self, message: str, key: str) -> None:
        """Log `message` the first time this run asks for it under `key`."""
        self._logger_once.log(message, key=key)

    def finish(self) -> None:
        """Drop a pending wake-up and mark the run as history, so that the crawler can start another one."""
        self._finished = True

        if self._wake_timer is not None:
            self._wake_timer.cancel()
