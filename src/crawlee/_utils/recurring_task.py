from __future__ import annotations

import asyncio
import inspect
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta
    from types import TracebackType

    from typing_extensions import Self

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

        Run the function in a loop, waiting for the configured delay between executions.
        Supports both synchronous and asynchronous functions.
        """
        sleep_time_secs = self.delay.total_seconds()
        while True:
            await self.func() if inspect.iscoroutinefunction(self.func) else self.func()
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
