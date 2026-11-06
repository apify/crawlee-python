from __future__ import annotations

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
        self._finished = False

        self.failed = False
        """Whether a request has failed while the crawler aborts on an error."""

        self.handled_request = False
        """Whether this run has marked any request as handled."""

        self.previous_state: TStatisticsState | None = None
        """The statistics state at the last status update of this run."""

    @property
    def is_live(self) -> bool:
        """Whether the `BasicCrawler.run` call this belongs to is still executing."""
        return not self._finished

    @property
    def stop_requested(self) -> bool:
        """Whether a graceful shutdown of this run has been requested."""
        return self._stop_requested

    async def dispatch_requests(self) -> None:
        """Run the task loop until the crawl is over. The concurrency system has to be active by now."""
        await self._autoscaled_pool.run()

    def stop(self) -> None:
        """Ask the run to stop taking new requests."""
        self._stop_requested = True

    def log_once(self, message: str, key: str) -> None:
        """Log `message` the first time this run asks for it under `key`."""
        self._logger_once.log(message, key=key)

    def finish(self) -> None:
        """Mark the run as history, so that the crawler can start another one."""
        self._finished = True
