# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/core/src/autoscaling/autoscaled_pool.ts

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from datetime import timedelta
from logging import getLogger
from typing import TYPE_CHECKING, Awaitable, Callable

from crawlee._utils.recurring_task import RecurringTask

if TYPE_CHECKING:
    from crawlee.autoscaling import SystemStatus

__all__ = ['ConcurrencySettings', 'AutoscaledPool']

logger = getLogger(__name__)


class AbortError(Exception):
    """Raised when an AutoscaledPool run is aborted. Not for direct use."""


class ConcurrencySettings:
    """Concurrency settings for AutoscaledPool."""

    def __init__(
        self,
        min_concurrency: int = 1,
        max_concurrency: int = 200,
        max_tasks_per_minute: float = float('inf'),
        desired_concurrency: int | None = None,
    ) -> None:
        """Initialize the ConcurrencySettings.

        Args:
            min_concurrency: The minimum number of tasks running in parallel. If you set this value too high
                with respect to the available system memory and CPU, your code might run extremely slow or crash.

            max_concurrency: The maximum number of tasks running in parallel.

            max_tasks_per_minute: The maximum number of tasks per minute the pool can run. By default, this is set
                to infinity, but you can pass any positive, non-zero number.

            desired_concurrency: The desired number of tasks that should be running parallel on the start of the pool,
                if there is a large enough supply of them. By default, it is `min_concurrency`.
        """
        if desired_concurrency is not None and desired_concurrency < 1:
            raise ValueError('desired_concurrency must be 1 or larger')

        if min_concurrency < 1:
            raise ValueError('min_concurrency must be 1 or larger')

        if max_concurrency < min_concurrency:
            raise ValueError('max_concurrency cannot be less than min_concurrency')

        if max_tasks_per_minute <= 0:
            raise ValueError('max_tasks_per_minute must be positive')

        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.desired_concurrency = desired_concurrency if desired_concurrency is not None else min_concurrency
        self.max_tasks_per_minute = max_tasks_per_minute


class _AutoscaledPoolRun:
    def __init__(self) -> None:
        self.worker_tasks = list[asyncio.Task]()
        """A list of worker tasks currently in progress"""

        self.worker_tasks_updated = asyncio.Event()
        self.cleanup_done = asyncio.Event()
        self.result: asyncio.Future = asyncio.Future()


class AutoscaledPool:
    """Manages a pool of asynchronous resource-intensive tasks that are executed in parallel.

    The pool only starts new tasks if there is enough free CPU and memory available. If an exception is thrown in
    any of the tasks, it is propagated and the pool is stopped.
    """

    def __init__(
        self,
        *,
        system_status: SystemStatus,
        concurrency_settings: ConcurrencySettings | None = None,
        run_task_function: Callable[[], Awaitable],
        is_task_ready_function: Callable[[], Awaitable[bool]],
        is_finished_function: Callable[[], Awaitable[bool]],
        task_timeout: timedelta | None = None,
        autoscale_interval: timedelta = timedelta(seconds=10),
        logging_interval: timedelta = timedelta(minutes=1),
        desired_concurrency_ratio: float = 0.9,
        scale_up_step_ratio: float = 0.05,
        scale_down_step_ratio: float = 0.05,
    ) -> None:
        """Initialize the AutoscaledPool.

        Args:
            system_status: Provides data about system utilization (load).

            run_task_function: A function that performs an asynchronous resource-intensive task.

            is_task_ready_function: A function that indicates whether `run_task_function` should be called. This
                function is called every time there is free capacity for a new task and it should indicate whether
                it should start a new task or not by resolving to either `True` or `False`. Besides its obvious use,
                it is also useful for task throttling to save resources.

            is_finished_function: A function that is called only when there are no tasks to be processed. If it
                resolves to `True` then the pool's run finishes. Being called only when there are no tasks being
                processed means that as long as `is_task_ready_function` keeps resolving to `True`,
                `is_finished_function` will never be called. To abort a run, use the `abort` method.

            task_timeout: Timeout in which the `run_task_function` needs to finish.

            autoscale_interval: Defines how often the pool should attempt to adjust the desired concurrency based on
                the latest system status. Setting it lower than 1 might have a severe impact on performance. We suggest
                using a value from 5 to 20.

            logging_interval: Specifies a period in which the instance logs its state, in seconds.

            desired_concurrency_ratio: Minimum level of desired concurrency to reach before more scaling up is allowed.

            scale_up_step_ratio: Defines the fractional amount of desired concurrency to be added with each scaling up.

            scale_down_step_ratio: Defines the amount of desired concurrency to be subtracted with each scaling down.

            concurrency_settings: Settings of concurrency levels
        """
        self._system_status = system_status

        self._run_task_function = run_task_function
        self._is_task_ready_function = is_task_ready_function
        self._is_finished_function = is_finished_function

        self._task_timeout = task_timeout

        self._logging_interval = logging_interval
        self._log_system_status_task = RecurringTask(self._log_system_status, logging_interval)

        self._autoscale_task = RecurringTask(self._autoscale, autoscale_interval)

        if desired_concurrency_ratio < 0 or desired_concurrency_ratio > 1:
            raise ValueError('desired_concurrency_ratio must be between 0 and 1 (non-inclusive)')

        self._desired_concurrency_ratio = desired_concurrency_ratio

        concurrency_settings = concurrency_settings or ConcurrencySettings()

        self._desired_concurrency = concurrency_settings.desired_concurrency
        self._max_concurrency = concurrency_settings.max_concurrency
        self._min_concurrency = concurrency_settings.min_concurrency

        self._scale_up_step_ratio = scale_up_step_ratio
        self._scale_down_step_ratio = scale_down_step_ratio

        self._max_tasks_per_minute = concurrency_settings.max_tasks_per_minute
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

        min_current_concurrency = math.floor(self._desired_concurrency_ratio * self.current_concurrency)
        should_scale_up = (
            status.is_system_idle
            and self._desired_concurrency < self._max_concurrency
            and self.current_concurrency >= min_current_concurrency
        )

        should_scale_down = not status.is_system_idle and self._desired_concurrency > self._min_concurrency

        if should_scale_up:
            step = math.ceil(self._scale_up_step_ratio * self._desired_concurrency)
            self._desired_concurrency = min(self._max_concurrency, self._desired_concurrency + step)
        elif should_scale_down:
            step = math.ceil(self._scale_down_step_ratio * self._desired_concurrency)
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
                timeout=self._task_timeout.total_seconds() if self._task_timeout is not None else None,
            )
        except asyncio.TimeoutError:
            timeout_str = self._task_timeout.total_seconds() if self._task_timeout is not None else '*not set*'
            logger.warning(f'Task timed out after {timeout_str} seconds')
        finally:
            logger.debug('Worker task finished')
