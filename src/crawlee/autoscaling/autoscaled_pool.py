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
    from crawlee.autoscaling.system_status import SystemStatus

logger = getLogger(__name__)


class AutoscaledPool:
    """Manages a pool of asynchronous resource-intensive tasks that are executed in parallel.

    The pool only starts new tasks if there is enough free CPU and memory available.
    """

    def __init__(
        self: AutoscaledPool,
        *,
        system_status: SystemStatus,
        run_task_function: Callable[..., Awaitable],
        is_task_ready_function: Callable[..., bool],
        is_finished_function: Callable[..., bool],
        task_timeout: timedelta | None = None,
        autoscale_interval: timedelta = timedelta(seconds=10),
        logging_interval: timedelta = timedelta(minutes=1),
        desired_concurrency: int | None = None,
        desired_concurrency_ratio: float = 0.9,
        min_concurrency: int = 1,
        max_concurrency: int = 200,
        scale_up_step_ratio: float = 0.05,
        scale_down_step_ratio: float = 0.05,
        max_tasks_per_minute: int | None = None,
    ) -> None:
        """Initialize the AutoscaledPool.

        Args:
            system_status: Provides data about system utilization (load).

            run_task_function: A function that performs an asynchronous resource-intensive task.

            is_task_ready_function: A function that indicates whether `run_task_function` should be called. This
            function is called every time there is free capacity for a new task and it should indicate whether it should
            start a new task or not by resolving to either `True` or `False`. Besides its obvious use, it is also useful
            for task throttling to save resources. is_finished_function: A function that is called only when there are
            no tasks to be processed. If it resolves to `True` then the pool's run finishes. Being called only when
            there are no tasks being processed means that as long as `is_task_ready_function()` keeps resolving to
            `True`, `is_finished_function()` will never be called. To abort a run, use the `abort` method.

            is_finished_function: A function that is called only when there are no tasks to be processed. If it resolves
            to `true` then the pool's run finishes. Being called only when there are no tasks being processed means
            that as long as `isTaskReadyFunction()` keeps resolving to `true`, `isFinishedFunction()` will never
            be called. To abort a run, use the `abort` method.

            task_timeout: Timeout in which the `run_task_function` needs to finish.

            autoscale_interval: Defines how often the pool should attempt to adjust the desired concurrency based on
                the latest system status. Setting it lower than 1 might have a severe impact on performance. We suggest
                using a value from 5 to 20.

            logging_interval: Specifies a period in which the instance logs its state, in seconds.

            desired_concurrency: The desired number of tasks that should be running parallel on the start of the pool,
                if there is a large enough supply of them. By default, it is `min_concurrency`.

            desired_concurrency_ratio: Minimum level of desired concurrency to reach before more scaling up is allowed.

            min_concurrency: The minimum number of tasks running in parallel. If you set this value too high with
                respect to the available system memory and CPU, your code might run extremely slow or crash.

            max_concurrency: The maximum number of tasks running in parallel.

            scale_up_step_ratio: Defines the fractional amount of desired concurrency to be added with each scaling up.

            scale_down_step_ratio: Defines the amount of desired concurrency to be subtracted with each scaling down.

            max_tasks_per_minute: The maximum number of tasks per minute the pool can run. By default, this is set to
                `Infinity`, but you can pass any positive, non-zero integer.
        """
        self._system_status = system_status

        self._run_task_function = run_task_function
        self._is_task_ready_function = is_task_ready_function
        self._is_finished_function = is_finished_function

        self._task_timeout = task_timeout

        self._logging_interval = logging_interval
        self._log_system_status_task = RecurringTask(self._log_system_status, logging_interval)

        self._autoscale_task = RecurringTask(self._autoscale, autoscale_interval)

        self._worker_tasks = list[tuple[asyncio.Task, asyncio.Event]]()
        """A list of workers tasks and events to signal that the task should terminate"""

        self._worker_tasks_updated = asyncio.Event()
        self._tasks_for_cleanup = list[asyncio.Task]()

        if desired_concurrency is not None and desired_concurrency < 1:
            raise ValueError('desired_concurrency must be 1 or larger')

        if min_concurrency < 1:
            raise ValueError('min_concurrency must be 1 or larger')

        if max_concurrency < min_concurrency:
            raise ValueError('max_concurrency cannot be less than min_concurrency')

        if desired_concurrency_ratio < 0 or desired_concurrency_ratio > 1:
            raise ValueError('desired_concurrency_ratio must be between 0 and 1 (non-inclusive)')

        self._desired_concurrency_ratio = desired_concurrency_ratio
        self._desired_concurrency = desired_concurrency if desired_concurrency is not None else min_concurrency
        self._max_concurrency = max_concurrency
        self._min_concurrency = min_concurrency

        self._current_concurrency = 0

        self._scale_up_step_ratio = scale_up_step_ratio
        self._scale_down_step_ratio = scale_down_step_ratio

        self._max_tasks_per_minute = max_tasks_per_minute
        self._is_paused = False

    async def run(self: AutoscaledPool) -> None:
        """Start the autoscaled pool and return when all tasks are completed and `is_finished_function` returns True.

        If there is an exception in one of the tasks, it will be re-raised.
        """
        self._ensure_desired_concurrency()
        self._autoscale_task.start()
        self._log_system_status_task.start()

        logger.debug('Starting the pool')

        try:
            while not self._is_finished_function():
                wait_for_workers_update = asyncio.create_task(
                    self._worker_tasks_updated.wait(), name='wait for worker tasks update'
                )
                wait_for_worker_tasks = asyncio.create_task(
                    asyncio.wait([task for task, _ in self._worker_tasks], return_when=asyncio.FIRST_EXCEPTION),
                    name='wait for worker tasks to complete',
                )

                self._worker_tasks_updated.clear()

                try:
                    await asyncio.wait(
                        [wait_for_workers_update, wait_for_worker_tasks],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    if not wait_for_worker_tasks.done():
                        wait_for_worker_tasks.cancel()

                    if not wait_for_workers_update.done():
                        wait_for_workers_update.cancel()

                    for task, _ in self._worker_tasks:
                        if task.done():
                            exception = task.exception()
                            if exception is not None:
                                raise exception
        finally:
            with suppress(asyncio.CancelledError):
                await self._autoscale_task.stop()
            with suppress(asyncio.CancelledError):
                await self._log_system_status_task.stop()

            self._desired_concurrency = 0
            self._ensure_desired_concurrency()

            for task in self._tasks_for_cleanup:
                if not task.done():
                    with suppress(asyncio.CancelledError):
                        await task

            logger.debug('Pool cleanup finished')

    async def abort(self: AutoscaledPool) -> None:
        """Interrupt the autoscaled pool and all the tasks in progress."""
        self._is_paused = True
        await self._autoscale_task.stop()

        for task, _ in self._worker_tasks:
            task.cancel()

    def pause(self: AutoscaledPool) -> None:
        """Pause the autoscaled pool so that it does not start new tasks."""
        self._is_paused = True

    def resume(self: AutoscaledPool) -> None:
        """Resume a paused autoscaled pool so that it continues starting new tasks."""
        self._is_paused = False

    @property
    def desired_concurrency(self: AutoscaledPool) -> int:
        """The current desired concurrency, possibly updated by the pool according to system load."""
        return self._desired_concurrency

    def _autoscale(self: AutoscaledPool) -> None:
        status = self._system_status.get_historical_status()
        min_current_concurrency = math.floor(self._desired_concurrency_ratio * self._current_concurrency)

        if (
            status.is_system_idle
            and self._desired_concurrency < self._max_concurrency
            and self._current_concurrency >= min_current_concurrency
        ):
            step = math.ceil(self._scale_up_step_ratio * self._desired_concurrency)
            self._desired_concurrency = min(self._max_concurrency, self._desired_concurrency + step)
        elif not status.is_system_idle and self._desired_concurrency > self._min_concurrency:
            step = math.ceil(self._scale_down_step_ratio * self._desired_concurrency)
            self._desired_concurrency = max(self._min_concurrency, self._desired_concurrency - step)

        self._ensure_desired_concurrency()

    def _ensure_desired_concurrency(self: AutoscaledPool) -> None:
        if len(self._worker_tasks) > self._desired_concurrency:
            for _ in range(len(self._worker_tasks) - self._desired_concurrency):
                task, terminate_event = self._worker_tasks.pop()
                self._mark_task_for_cleanup(task)
                terminate_event.set()
                self._worker_tasks_updated.set()

        elif len(self._worker_tasks) < self._desired_concurrency:
            for i in range(len(self._worker_tasks), self._desired_concurrency):
                terminate_event = asyncio.Event()
                task = asyncio.create_task(self._worker_task(terminate_event), name=f'worker task #{i + 1}')
                self._worker_tasks.append((task, terminate_event))
                self._worker_tasks_updated.set()

    def _mark_task_for_cleanup(self: AutoscaledPool, task: asyncio.Task) -> None:
        def cleanup(_: asyncio.Future) -> None:
            task.exception()
            self._tasks_for_cleanup.remove(task)

        self._tasks_for_cleanup.append(task)
        task.add_done_callback(cleanup)

    def _log_system_status(self: AutoscaledPool) -> None:
        system_status = self._system_status.get_historical_status()

        logger.info(
            f'current_concurrency = {self._current_concurrency}; '
            f'desired_concurrency = {self._desired_concurrency}; '
            f'{system_status!s}'
        )

    async def _worker_task(self: AutoscaledPool, terminate_event: asyncio.Event) -> None:
        while not self._is_finished_function():
            if self._max_tasks_per_minute is not None and self._desired_concurrency > 0:
                delay = 60 / self._max_tasks_per_minute / self._desired_concurrency
            else:
                delay = 0

            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(terminate_event.wait(), delay)

            if terminate_event.is_set() or self._is_finished_function():
                break

            if self._is_paused:
                logger.debug('Paused - not executing a task')
                continue

            if not self._is_task_ready_function():
                logger.debug('No task is ready yet')
                continue

            self._current_concurrency += 1
            try:
                await asyncio.wait_for(
                    self._run_task_function(),
                    timeout=self._task_timeout.total_seconds() if self._task_timeout is not None else None,
                )
            except asyncio.TimeoutError:
                timeout_str = self._task_timeout.total_seconds() if self._task_timeout is not None else '*not set*'
                logger.warning(f'Task timed out after {timeout_str} seconds')
            finally:
                self._current_concurrency -= 1

        logger.debug('Worker task finished')
