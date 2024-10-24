from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from datetime import timedelta
    from logging import Logger

T = TypeVar('T')


async def wait_for(
    operation: Callable[[], Awaitable[T]],
    *,
    timeout: timedelta,
    timeout_message: str | None = None,
    max_retries: int = 1,
    logger: Logger,
) -> T:
    """Wait for an async operation to complete.

    If the wait times out, `TimeoutError` is raised and the future is cancelled.
    Optionally retry on error.

    Args:
        operation: A function that returns the future to wait for.
        timeout: How long should we wait before cancelling the future.
        timeout_message: Message to be included in the `TimeoutError` in case of timeout.
        max_retries: How many times should the operation be attempted.
        logger: Used to report information about retries as they happen.
    """
    for iteration in range(1, max_retries + 1):
        try:
            return await asyncio.wait_for(operation(), timeout.total_seconds())
        except asyncio.TimeoutError as ex:  # noqa: PERF203
            raise asyncio.TimeoutError(timeout_message) from ex
        except Exception as e:
            if iteration == max_retries:
                raise

            logger.warning(f'{e!s}: retrying ({iteration}/{max_retries})')

    raise RuntimeError('Unreachable code')


async def wait_for_all_tasks_for_finish(
    tasks: Sequence[asyncio.Task],
    *,
    logger: Logger,
    timeout: timedelta | None = None,
) -> None:
    """Wait for all tasks to finish or until the timeout is reached.

    Args:
        tasks: A sequence of asyncio tasks to wait for.
        logger: Logger to use for reporting.
        timeout: How long should we wait before cancelling the tasks.
    """
    if not tasks:
        return

    timeout_secs = timeout.total_seconds() if timeout else None
    try:
        _, pending = await asyncio.wait(tasks, timeout=timeout_secs)
        if pending:
            logger.warning('Waiting timeout reached; canceling unfinished tasks.')
    except asyncio.CancelledError:
        logger.warning('Asyncio wait was cancelled; canceling unfinished tasks.')
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            # If task is done, access the result to clear any exceptions
            else:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f'Task raised an exception: {e}')
