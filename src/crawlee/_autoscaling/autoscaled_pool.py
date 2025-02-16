# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/autoscaled_pool.ts

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Callable

from crawlee._types import ConcurrencySettings
from crawlee._utils.docs import docs_group
from crawlee._utils.recurring_task import RecurringTask

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from crawlee._autoscaling import SystemStatus

logger = getLogger(__name__)


class AbortError(Exception):
    """Raised when an AutoscaledPool run is aborted. Not for direct use."""


class _AutoscaledPoolRun:
    def __init__(self) -> None:
        self.worker_tasks = list[asyncio.Task]()
        """A list of worker tasks currently in progress"""

        self.worker_tasks_updated = asyncio.Event()
        self.cleanup_done = asyncio.Event()
        self.result: asyncio.Future = asyncio.Future()


@docs_group('Classes')
class AutoscaledPool:
    """Manages a pool of asynchronous resource-intensive tasks that are executed in parallel.

    The pool only starts new tasks if there is enough free CPU and memory available. If an exception is thrown in
    any of the tasks, it is propagated and the pool is stopped.
    """

    _AUTOSCALE_INTERVAL = timedelta(seconds=10)
    """Interval at which the autoscaled pool adjusts the desired concurrency based on the latest system status."""

    _LOGGING_INTERVAL = timedelta(minutes=1)
    """Interval at which the autoscaled pool logs its current state."""

    _DESIRED_CONCURRENCY_RATIO = 0.9
    """Minimum ratio of desired concurrency that must be reached before allowing further scale-up."""

    _SCALE_UP_STEP_RATIO = 0.05
    """Fraction of desired concurrency to add during each scale-up operation."""

    _SCALE_DOWN_STEP_RATIO = 0.05
    """Fraction of desired concurrency to remove during each scale-down operation."""

    _TASK_TIMEOUT: timedelta | None = None
    """Timeout within which the `run_task_function` must complete."""

    def __init__(
        self,
        *,
        system_status: SystemStatus,
        concurrency_settings: ConcurrencySettings | None = None,
        run_task_function: Callable[[], Awaitable],
        is_task_ready_function: Callable[[], Awaitable[bool]],
        is_finished_function: Callable[[], Awaitable[bool]],
    ) -> None:
        """A default constructor.

        Args:
            system_status: Provides data about system utilization (load).
            concurrency_settings: Settings of concurrency levels.
            run_task_function: A function that performs an asynchronous resource-intensive task.
            is_task_ready_function: A function that indicates whether `run_task_function` should be called. This
                function is called every time there is free capacity for a new task and it should indicate whether
                it should start a new task or not by resolving to either `True` or `False`. Besides its obvious use,
                it is also useful for task throttling to save resources.
            is_finished_function: A function that is called only when there are no tasks to be processed. If it
                resolves to `True` then the pool's run finishes. Being called only when there are no tasks being
                processed means that as long as `is_task_ready_function` keeps resolving to `True`,
                `is_finished_function` will never be called. To abort a run, use the `abort` method.
        """
        concurrency_settings = concurrency_settings or ConcurrencySettings()

        self._system_status = system_status
        self._run_task_function = run_task_function
        self._is_task_ready_function = is_task_ready_function
        self._is_finished_function = is_finished_function
        self._desired_concurrency = concurrency_settings.desired_concurrency
        self._max_concurrency = concurrency_settings.max_concurrency
        self._min_concurrency = concurrency_settings.min_concurrency
        self._max_tasks_per_minute = concurrency_settings.max_tasks_per_minute

        self._log_system_status_task = RecurringTask(self._log_system_status, self._LOGGING_INTERVAL)
        self._autoscale_task = RecurringTask(self._autoscale, self._AUTOSCALE_INTERVAL)

        self._is_paused = False
        self._current_run: _AutoscaledPoolRun | None = None

    async def run(self) -> None:
        """Start the autoscaled pool and return when all tasks are completed and `is_finished_function` returns True.

        If there is an exception in one of the tasks, it will be re-raised.
        """
        if self._current_run is not None:
            raise RuntimeError('The pool is already running')

        run = _AutoscaledPoolRun()
        self._current_run = run

        logger.debug('Starting the pool')

        self._autoscale_task.start()
        self._log_system_status_task.start()

        orchestrator = asyncio.create_task(
            self._worker_task_orchestrator(run), name='autoscaled pool worker task orchestrator'
        )

        try:
            await run.result
        except AbortError:
            orchestrator.cancel()
            for task in run.worker_tasks:
                if not task.done():
                    task.cancel()
        finally:
            with suppress(asyncio.CancelledError):
                await self._autoscale_task.stop()
            with suppress(asyncio.CancelledError):
                await self._log_system_status_task.stop()

            if not orchestrator.done():
                orchestrator.cancel()
            elif not orchestrator.cancelled() and orchestrator.exception() is not None:
                logger.error('Exception in worker task orchestrator', exc_info=orchestrator.exception())

            logger.info('Waiting for remaining tasks to finish')

            for task in run.worker_tasks:
                if not task.done():
                    with suppress(BaseException):
                        await task

            run.cleanup_done.set()
            self._current_run = None

            logger.debug('Pool cleanup finished')

    async def abort(self) -> None:
        """Interrupt the autoscaled pool and all the tasks in progress."""
        if not self._current_run:
            raise RuntimeError('The pool is not running')

        self._current_run.result.set_exception(AbortError())
        await self._current_run.cleanup_done.wait()

    def pause(self) -> None:
        """Pause the autoscaled pool so that it does not start new tasks."""
        self._is_paused = True

    def resume(self) -> None:
        """Resume a paused autoscaled pool so that it continues starting new tasks."""
        self._is_paused = False

    @property
    def desired_concurrency(self) -> int:
        """The current desired concurrency, possibly updated by the pool according to system load."""
        return self._desired_concurrency

    @property
    def current_concurrency(self) -> int:
        """The number of concurrent tasks in progress."""
        if self._current_run is None:
            return 0

        return len(self._current_run.worker_tasks)

    def _autoscale(self) -> None:
        """Inspect system load status and adjust desired concurrency if necessary. Do not call directly."""
        status = self._system_status.get_historical_system_info()

        min_current_concurrency = math.floor(self._DESIRED_CONCURRENCY_RATIO * self.desired_concurrency)
        should_scale_up = (
            status.is_system_idle
            and self._desired_concurrency < self._max_concurrency
            and self.current_concurrency >= min_current_concurrency
        )

        should_scale_down = not status.is_system_idle and self._desired_concurrency > self._min_concurrency

        if should_scale_up:
            step = math.ceil(self._SCALE_UP_STEP_RATIO * self._desired_concurrency)
            self._desired_concurrency = min(self._max_concurrency, self._desired_concurrency + step)
        elif should_scale_down:
            step = math.ceil(self._SCALE_DOWN_STEP_RATIO * self._desired_concurrency)
            self._desired_concurrency = max(self._min_concurrency, self._desired_concurrency - step)

    def _log_system_status(self) -> None:
        system_status = self._system_status.get_historical_system_info()

        logger.info(
            f'current_concurrency = {self.current_concurrency}; '
            f'desired_concurrency = {self.desired_concurrency}; '
            f'{system_status!s}'
        )

    async def _worker_task_orchestrator(self, run: _AutoscaledPoolRun) -> None:
        """Launches worker tasks whenever there is free capacity and a task is ready.

        Exits when `is_finished_function` returns True.
        """
        finished = False

        try:
            while not (finished := await self._is_finished_function()) and not run.result.done():
                run.worker_tasks_updated.clear()

                current_status = self._system_status.get_current_system_info()
                if not current_status.is_system_idle:
                    logger.debug('Not scheduling new tasks - system is overloaded')
                elif self._is_paused:
                    logger.debug('Not scheduling new tasks - the autoscaled pool is paused')
                elif self.current_concurrency >= self.desired_concurrency:
                    logger.debug('Not scheduling new tasks - already running at desired concurrency')
                elif not await self._is_task_ready_function():
                    logger.debug('Not scheduling new task - no task is ready')
                else:
                    logger.debug('Scheduling a new task')
                    worker_task = asyncio.create_task(self._worker_task(), name='autoscaled pool worker task')
                    worker_task.add_done_callback(lambda task: self._reap_worker_task(task, run))
                    run.worker_tasks.append(worker_task)

                    if math.isfinite(self._max_tasks_per_minute):
                        await asyncio.sleep(60 / self._max_tasks_per_minute)

                    continue

                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(run.worker_tasks_updated.wait(), timeout=0.5)
        finally:
            if finished:
                logger.debug('`is_finished_function` reports that we are finished')
            elif run.result.done() and run.result.exception() is not None:
                logger.debug('Unhandled exception in `run_task_function`')

            if run.worker_tasks:
                logger.debug('Terminating - waiting for tasks to complete')
                await asyncio.wait(run.worker_tasks, return_when=asyncio.ALL_COMPLETED)
                logger.debug('Worker tasks finished')
            else:
                logger.debug('Terminating - no running tasks to wait for')

            if not run.result.done():
                run.result.set_result(object())

    def _reap_worker_task(self, task: asyncio.Task, run: _AutoscaledPoolRun) -> None:
        """A callback for finished worker tasks.

        - It interrupts the run in case of an exception,
        - keeps track of tasks in progress,
        - notifies the orchestrator
        """
        run.worker_tasks_updated.set()
        run.worker_tasks.remove(task)

        if not task.cancelled() and (exception := task.exception()) and not run.result.done():
            run.result.set_exception(exception)

    async def _worker_task(self) -> None:
        try:
            await asyncio.wait_for(
                self._run_task_function(),
                timeout=self._TASK_TIMEOUT.total_seconds() if self._TASK_TIMEOUT is not None else None,
            )
        except asyncio.TimeoutError:
            timeout_str = self._TASK_TIMEOUT.total_seconds() if self._TASK_TIMEOUT is not None else '*not set*'
            logger.warning(f'Task timed out after {timeout_str} seconds')
        finally:
            logger.debug('Worker task finished')
