from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Sequence
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
        except TimeoutError as ex:
            raise TimeoutError(timeout_message) from ex
        except Exception as e:
            if iteration == max_retries:
                raise

            logger.warning(f'{e!s}: retrying ({iteration}/{max_retries})')

    raise RuntimeError('Unreachable code')


async def wait_for_all_tasks_to_finish(
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


@asynccontextmanager
async def task_group() -> AsyncGenerator[asyncio.TaskGroup]:
    """Open an `asyncio.TaskGroup` that raises the first failure as is, not wrapped in an exception group.

    A failure in a task or in the body cancels the remaining tasks, as with a plain `TaskGroup`, so callers catch the
    same exception types as when awaiting the work directly.
    """
    body_error: BaseException | None = None
    error: BaseException | None = None
    try:
        async with asyncio.TaskGroup() as tg:
            try:
                yield tg
            except BaseException as exc:
                # A body raising while it is being cancelled is reacting to an earlier task failure, not failing first.
                if not (current := asyncio.current_task()) or not current.cancelling():
                    body_error = exc
                raise
    except BaseExceptionGroup as group:
        # A failing body is the first failure, but `TaskGroup` lists it after errors the tasks raise while cancelled.
        error = body_error if body_error in group.exceptions else group.exceptions[0]

    # Raised outside the `except` block, so the group does not become the exception's context.
    if error is not None:
        raise error


async def run_all(*coros: Coroutine[Any, Any, T]) -> list[T]:
    """Run the coroutines concurrently and return their results in order.

    Unlike `asyncio.gather`, the first failure cancels the coroutines that are still running before it is raised.
    """
    async with task_group() as tg:
        tasks = [tg.create_task(coro) for coro in coros]
    return [task.result() for task in tasks]
