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
        self.system_status = system_status

        self._run_task_function = run_task_function
        self._is_task_ready_function = is_task_ready_function
        self._is_finished_function = is_finished_function

        self._task_timeout = task_timeout

        self._logging_interval = logging_interval
        self._log_system_status_task = RecurringTask(self._log_system_status, logging_interval)

        self._autoscale_task = RecurringTask(self._autoscale, autoscale_interval)

        self._worker_tasks = list[asyncio.Task]()
        self._worker_tasks_updated = asyncio.Event()

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
                    asyncio.wait(self._worker_tasks, return_when=asyncio.FIRST_EXCEPTION),
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

                    for task in self._worker_tasks:
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

            logger.debug('Pool cleanup finished')

    async def abort(self: AutoscaledPool) -> None:
        """Interrupt the autoscaled pool and all the tasks in progress."""
        self._is_paused = True
        await self._autoscale_task.stop()

        for task in self._worker_tasks:
            task.cancel()

    async def pause(self: AutoscaledPool) -> None:
        """Pause the autoscaled pool so that it does not start new tasks."""
        self._is_paused = True

    async def resume(self: AutoscaledPool) -> None:
        """Resume a paused autoscaled pool so that it continues starting new tasks."""
        self._is_paused = False

    @property
    def desired_concurrency(self: AutoscaledPool) -> int:
        """The current desired concurrency, possibly updated by the pool according to system load."""
        return self._desired_concurrency

    def _autoscale(self: AutoscaledPool) -> None:
        status = self.system_status.get_historical_status()
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
                self._worker_tasks.pop().cancel()
                self._worker_tasks_updated.set()

        elif len(self._worker_tasks) < self._desired_concurrency:
            for i in range(len(self._worker_tasks), self._desired_concurrency):
                task = asyncio.create_task(self._worker_task(), name=f'worker task #{i + 1}')
                self._worker_tasks.append(task)
                self._worker_tasks_updated.set()

    def _log_system_status(self: AutoscaledPool) -> None:
        logger.info(
            f'current_concurrency = {self._current_concurrency}; desired_concurrency = {self._desired_concurrency}'
        )

    async def _worker_task(self: AutoscaledPool) -> None:
        while not self._is_finished_function():
            if self._max_tasks_per_minute is not None:
                delay = 60 / self._max_tasks_per_minute / self._desired_concurrency
            else:
                delay = 0

            await asyncio.sleep(delay)

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
                logger.warning(
                    f"Task timed out after {
                        self._task_timeout.total_seconds() if self._task_timeout is not None else '*not set*'
                    } seconds"
                )
            finally:
                self._current_concurrency -= 1

        logger.debug('Worker task finished')
