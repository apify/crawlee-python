from __future__ import annotations

import asyncio
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta

logger = getLogger(__name__)


class RecurringTask:
    """Class for creating and managing recurring tasks.

    Attributes:
        func: The function to be executed repeatedly.
        delay: The time delay (in seconds) between function calls.
        task: The underlying task object.
    """

    def __init__(self, func: Callable, delay: timedelta) -> None:
        logger.debug(f'Calling RecurringTask.__init__(func={func.__name__}, delay={delay})...')
        self.func = func
        self.delay = delay
        self.task: asyncio.Task | None = None

    async def _wrapper(self) -> None:
        """Continuously execute the provided function with the specified delay.

        Run the function in a loop, waiting for the configured delay between executions.
        Supports both synchronous and asynchronous functions.
        """
        sleep_time_secs = self.delay.total_seconds()
        while True:
            await self.func() if asyncio.iscoroutinefunction(self.func) else self.func()
            await asyncio.sleep(sleep_time_secs)

    def start(self) -> None:
        """Start the recurring task execution."""
        self.task = asyncio.create_task(self._wrapper(), name=f'Task-recurring-{self.func.__name__}')

    async def stop(self) -> None:
        """Stop the recurring task execution."""
        if self.task:
            self.task.cancel()
            # Ensure the task has a chance to properly handle the cancellation and any potential exceptions.
            await asyncio.gather(self.task, return_exceptions=True)
