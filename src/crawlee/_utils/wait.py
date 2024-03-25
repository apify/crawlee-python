from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable, TypeVar

if TYPE_CHECKING:
    from datetime import timedelta
    from logging import Logger

T = TypeVar('T')


async def wait_for(
    fut: Awaitable[T], *, timeout: timedelta, timeout_message: str | None = None, max_retries: int = 1, logger: Logger
) -> T:
    for iteration in range(1, max_retries + 1):
        try:
            try:
                return await asyncio.wait_for(fut, timeout.total_seconds())
            except asyncio.TimeoutError as ex:
                raise asyncio.TimeoutError(timeout_message) from ex
        except Exception as e:  # noqa: PERF203
            if iteration == max_retries:
                raise

            logger.warning(f'{e!s}: retrying ({iteration}/{max_retries})')

    raise RuntimeError('Unreachable code')
