from __future__ import annotations

import asyncio
from contextlib import suppress
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import timedelta

    from ._types import ConcurrencyConsumer, ConcurrencySystemInterface

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


@docs_group('Autoscaling')
class AutoscaledPool:
    """Manages a pool of asynchronous resource-intensive tasks that are executed in parallel.

    The pool starts a new task only while the `ConcurrencySystemInterface` it borrows reports free capacity. That
    system is what monitors the load and scales the concurrency budget, so every concurrency option lives on it and
    the pool owns nothing but the task loop. If an exception is thrown in any of the tasks or in the pool's scheduling
    loop, it is propagated and the pool is stopped.
    """

    _TASK_TIMEOUT: timedelta | None = None
    """Timeout within which the `run_task_function` must complete."""

    def __init__(
        self,
        *,
        concurrency_system: ConcurrencySystemInterface,
        consumer: ConcurrencyConsumer,
        run_task_function: Callable[[str], Awaitable],
        is_task_ready_function: Callable[[], Awaitable[bool]],
        is_finished_function: Callable[[], Awaitable[bool]],
    ) -> None:
        """Initialize a new instance.

        Args:
            concurrency_system: The concurrency system that decides whether there is free compute for one more task,
                usually a `ConcurrencySystem`. Share one instance between pools to cap their combined concurrency.
            consumer: The identity of this pool, passed to the concurrency system on every capacity check and booking
                so that a shared system can tell pools apart. A system that allocates per consumer reports its `id`.
            run_task_function: A function that performs an asynchronous resource-intensive task. It receives the id
                the task is booked under, so that the work done inside can be matched with what the system sees.
            is_task_ready_function: A function that indicates whether `run_task_function` should be called. This
                function is called every time there is free capacity for a new task and it should indicate whether
                it should start a new task or not by resolving to either `True` or `False`. Besides its obvious use,
                it is also useful for task throttling to save resources.
            is_finished_function: A function that is called only when there are no tasks to be processed and the
                pool is not paused. If it resolves to `True` then the pool's run finishes. Being called only when there
                are no tasks being processed means that as long as `is_task_ready_function` keeps resolving to `True`,
                `is_finished_function` will never be called. To abort a run, use the `abort` method.
        """
        self._concurrency_system = concurrency_system
        self._consumer = consumer
        self._run_task_function = run_task_function
        self._is_task_ready_function = is_task_ready_function
        self._is_finished_function = is_finished_function

        self._is_paused = False
        self._current_run: _AutoscaledPoolRun | None = None
        self._next_task_id = 0

    async def run(self) -> None:
        """Start the autoscaled pool and return when all tasks are completed and `is_finished_function` returns True.

        If a task or the pool's scheduling loop raises an exception, it will be re-raised.

        Raises:
            RuntimeError: If the pool is already running, or if the concurrency system it borrows is not active.
        """
        if self._current_run is not None:
            raise RuntimeError('The pool is already running')

        if not self._concurrency_system.active:
            raise RuntimeError(
                'The concurrency system this pool borrows is not active, so the system load would not be monitored '
                'and the concurrency would never be adjusted. Whoever creates a concurrency system owns its '
                'lifecycle: keep it open with `async with` until every pool borrowing it has finished.'
            )

        run = _AutoscaledPoolRun()
        self._current_run = run

        logger.debug('Starting the pool')

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
            if not orchestrator.done():
                orchestrator.cancel()
            elif not orchestrator.cancelled() and orchestrator.exception() is not None:
                logger.error('Exception in worker task orchestrator', exc_info=orchestrator.exception())

            logger.debug('Waiting for remaining tasks to finish')

            # A copy, as the done callback of every task that finishes meanwhile removes it from the list.
            for task in list(run.worker_tasks):
                if not task.done():
                    with suppress(BaseException):
                        await task

            # The done callbacks of tasks that just finished may not have run yet, so give the loop one tick.
            await asyncio.sleep(0)

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
    def concurrency_system(self) -> ConcurrencySystemInterface:
        """The concurrency system backing this pool, as supplied to the constructor."""
        return self._concurrency_system

    @property
    def desired_concurrency(self) -> int:
        """The number of tasks the concurrency system currently allows to run in parallel."""
        return self._concurrency_system.desired_concurrency

    @property
    def current_concurrency(self) -> int:
        """The number of tasks in flight, as reported by the concurrency system.

        A shared system may count the tasks of every pool borrowing it, not just this one's.
        """
        return self._concurrency_system.current_concurrency

    @property
    def own_concurrency(self) -> int:
        """The number of this pool's own tasks in flight, as opposed to the possibly shared `current_concurrency`."""
        if self._current_run is None:
            return 0

        return len(self._current_run.worker_tasks)

    async def _worker_task_orchestrator(self, run: _AutoscaledPoolRun) -> None:
        """Launch worker tasks whenever there is free capacity and a task is ready.

        Exits when `is_finished_function` returns True.
        """
        finished = False
        orchestrator_error: Exception | None = None

        try:
            while not run.result.done():
                run.worker_tasks_updated.clear()

                if self._is_paused:
                    logger.debug('Not scheduling new tasks - the autoscaled pool is paused')
                elif not self._concurrency_system.has_capacity_for_task(self._consumer):
                    logger.debug('Not scheduling new tasks - no free capacity')
                    # Another pool can keep a shared system saturated indefinitely, so this pool still has to notice
                    # that it has run out of work.
                    if finished := await self._maybe_finish(run):
                        break
                elif not await self._is_task_ready_function():
                    logger.debug('Not scheduling new task - no task is ready')
                    if finished := await self._maybe_finish(run):
                        break
                else:
                    task_id = str(self._next_task_id)
                    self._next_task_id += 1

                    # Booking is a separate step, because the capacity checked above may be gone by now - awaiting
                    # `is_task_ready_function` gave another pool sharing the system a chance to take the last slot.
                    if self._concurrency_system.try_register_task_start(self._consumer, task_id):
                        logger.debug('Scheduling a new task')
                        worker_task = asyncio.create_task(
                            self._worker_task(task_id), name='autoscaled pool worker task'
                        )
                        worker_task.add_done_callback(partial(self._reap_worker_task, run=run, task_id=task_id))
                        run.worker_tasks.append(worker_task)
                        continue

                    logger.debug('Not scheduling new task - the task was not accepted by the concurrency system')

                with suppress(TimeoutError):
                    await asyncio.wait_for(run.worker_tasks_updated.wait(), timeout=0.5)
        except Exception as exc:
            # Surface the error through `run.result` only once the cleanup below has awaited the worker tasks,
            # so that the caller does not observe the failure while tasks are still in flight.
            orchestrator_error = exc
        finally:
            if finished:
                logger.debug('`is_finished_function` reports that we are finished')
            elif run.result.done() and not run.result.cancelled() and run.result.exception() is not None:
                logger.debug('Unhandled exception in `run_task_function`')

            if run.worker_tasks:
                logger.debug('Terminating - waiting for tasks to complete')
                await asyncio.wait(run.worker_tasks, return_when=asyncio.ALL_COMPLETED)
                logger.debug('Worker tasks finished')
            else:
                logger.debug('Terminating - no running tasks to wait for')

            if not run.result.done():
                if orchestrator_error is not None:
                    run.result.set_exception(orchestrator_error)
                else:
                    run.result.set_result(object())
            elif orchestrator_error is not None:
                # A worker failure or an abort already decided the run, so this error has no way out.
                logger.error('Unpropagated exception in worker task orchestrator', exc_info=orchestrator_error)

    async def _maybe_finish(self, run: _AutoscaledPoolRun) -> bool:
        """Ask `is_finished_function` whether the run is over, but only while none of this pool's tasks is in flight.

        A running task can still produce new work, and with a shared system only this pool's own tasks count.
        """
        return not run.worker_tasks and await self._is_finished_function()

    def _reap_worker_task(self, task: asyncio.Task, *, run: _AutoscaledPoolRun, task_id: str) -> None:
        """Handle cleanup and tracking of a completed worker task.

        - Interrupt the run if the task encountered an exception.
        - Update the list of tasks in progress.
        - Notify the orchestrator about the task completion.
        - Return the task's slot to the concurrency system.
        """
        run.worker_tasks_updated.set()
        run.worker_tasks.remove(task)

        if not task.cancelled() and (exception := task.exception()) and not run.result.done():
            run.result.set_exception(exception)

        self._concurrency_system.register_task_end(self._consumer, task_id)

    async def _worker_task(self, task_id: str) -> None:
        try:
            await asyncio.wait_for(
                self._run_task_function(task_id),
                timeout=self._TASK_TIMEOUT.total_seconds() if self._TASK_TIMEOUT is not None else None,
            )
        except TimeoutError:
            if self._TASK_TIMEOUT is None:
                # Without a timeout, `wait_for` cannot time out - the error comes from the task function itself.
                raise
            logger.warning(f'Task timed out after {self._TASK_TIMEOUT.total_seconds()} seconds')
        finally:
            logger.debug('Worker task finished')
