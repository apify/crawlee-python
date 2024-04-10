from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

if TYPE_CHECKING:
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

    If the wait times out, TimeoutError is raised and the future is cancelled.
    Optionally retry on error.

    Args:
        operation: A function that returns the future to wait for
        timeout: How long should we wait before cancelling the future
        timeout_message: Message to be included in the TimeoutError in case of timeout
        max_retries: How many times should the operation be attempted
        logger: Used to report information about retries as they happen
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
