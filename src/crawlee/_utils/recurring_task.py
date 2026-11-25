from __future__ import annotations

import asyncio
import inspect
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta
    from types import TracebackType
    from typing import Self

logger = getLogger(__name__)


class RecurringTask:
    """Class for creating and managing recurring tasks.

    Attributes:
        func: The function to be executed repeatedly.
        delay: The time delay (in seconds) between function calls.
        task: The underlying task object.
    """

    def __init__(self, func: Callable, delay: timedelta, *, run_immediately: bool = True) -> None:
        self._func_name = func.__name__ if hasattr(func, '__name__') else func.__class__.__name__
        logger.debug('Calling RecurringTask.__init__(func={%s}, delay={%s})...', self._func_name, delay)
        self.func = func
        self.delay = delay
        self.task: asyncio.Task | None = None
        self._run_immediately = run_immediately

    async def __aenter__(self) -> Self:
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.stop()

    async def _wrapper(self) -> None:
        """Continuously execute the provided function with the specified delay.

        Run the function in a loop, waiting for the configured delay between executions, and also before the first one
        unless `run_immediately` is set. Supports both synchronous and asynchronous functions. An exception raised by
        the function is logged and does not stop the recurrence.
        """
        sleep_time_secs = self.delay.total_seconds()
        if not self._run_immediately:
            await asyncio.sleep(sleep_time_secs)

        while True:
            try:
                await self.func() if inspect.iscoroutinefunction(self.func) else self.func()
            except Exception:
                logger.exception('Recurring task `%s` failed; it will run again after the delay.', self._func_name)
            await asyncio.sleep(sleep_time_secs)

    def start(self) -> None:
        """Start the recurring task execution, cancelling the previous one if it still runs."""
        # A task of a closed event loop can't be cancelled, and it never runs again anyway.
        if self.task is not None and not self.task.done() and not self.task.get_loop().is_closed():
            self.task.cancel()

        self.task = asyncio.create_task(
            self._wrapper(),
            name=f'Task-recurring-{self._func_name}',
        )

    async def stop(self) -> None:
        """Stop the recurring task execution."""
        if self.task:
            self.task.cancel()
            # Ensure the task has a chance to properly handle the cancellation and any potential exceptions.
            await asyncio.gather(self.task, return_exceptions=True)
