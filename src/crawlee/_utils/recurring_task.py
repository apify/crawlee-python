from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from datetime import timedelta


class RecurringTask:
    """Class for creating and managing recurring tasks.

    Attributes:
        func: The function to be executed repeatedly.
        delay: The time delay (in seconds) between function calls.
        task: The underlying task object.
    """

    def __init__(self: RecurringTask, func: Callable, delay: timedelta) -> None:
        self.func = func
        self.delay = delay
        self.task: asyncio.Task | None = None

    async def _wrapper(self: RecurringTask) -> None:
        """Internal method that repeatedly executes the provided function with the specified delay."""
        sleep_time_secs = self.delay.total_seconds()
        while True:
            if asyncio.iscoroutinefunction(self.func):
                await self.func()
            else:
                self.func()
            await asyncio.sleep(sleep_time_secs)

    def start(self: RecurringTask) -> None:
        """Start the recurring task execution."""
        self.task = asyncio.create_task(self._wrapper(), name=f'Task-recurring-{self.func.__name__}')

    async def stop(self: RecurringTask) -> None:
        """Stop the recurring task execution."""
        if self.task:
            self.task.cancel()
            # Ensure the task has a chance to properly handle the cancellation and any potential exceptions.
            await asyncio.gather(self.task, return_exceptions=True)
