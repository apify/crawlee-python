from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from datetime import timedelta

logger = getLogger(__name__)


class RecurringTask:
    """Class for creating and managing recurring tasks.

    Attributes:
        func: The function to be executed repeatedly.
        delay: The time delay (in seconds) between function calls.
        task: The underlying task object.
    """

    def __init__(self: RecurringTask, func: Callable, delay: timedelta) -> None:
        logger.debug(f'Calling RecurringTask.__init__(func={func.__name__}, delay={delay})...')
        self.func = func
        self.delay = delay
        self.task: asyncio.Task | None = None

    async def _wrapper(self: RecurringTask) -> None:
        """Internal method that repeatedly executes the provided function with the specified delay."""
        logger.debug('Calling RecurringTask._wrapper()...')
        sleep_time_secs = self.delay.total_seconds()
        while True:
            logger.debug('RecurringTask._wrapper(): calling self.func()...')
            await self.func() if asyncio.iscoroutinefunction(self.func) else self.func()
            await asyncio.sleep(sleep_time_secs)

    def start(self: RecurringTask) -> None:
        """Start the recurring task execution."""
        logger.debug('Calling RecurringTask.start()...')
        self.task = asyncio.create_task(self._wrapper(), name=f'Task-recurring-{self.func.__name__}')

    async def stop(self: RecurringTask) -> None:
        """Stop the recurring task execution."""
        logger.debug('Calling RecurringTask.stop()...')
        if self.task:
            self.task.cancel()
            # Ensure the task has a chance to properly handle the cancellation and any potential exceptions.
            await asyncio.gather(self.task, return_exceptions=True)
