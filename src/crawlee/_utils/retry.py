from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import wraps
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

P = ParamSpec('P')
T = TypeVar('T')


def retry_on_error(
    *exception_types: type[Exception],
    max_attempts: int = 3,
    base_delay: timedelta = timedelta(milliseconds=500),
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Retry an async function with exponential backoff on specified exceptions.

    Args:
        *exception_types: Exception types to catch and retry on.
        max_attempts: Maximum number of attempts including the first one.
        base_delay: Base delay between retries; doubles on each subsequent attempt.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:

        if max_attempts < 1:
            raise ValueError('max_attempts must be at least 1')

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            base_delay_seconds = base_delay.total_seconds()
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:  # noqa: PERF203
                    if not isinstance(exc, exception_types) or attempt >= max_attempts - 1:
                        raise
                    await asyncio.sleep(base_delay_seconds * (2**attempt))
            raise RuntimeError('Unreachable')

        return wrapper

    return decorator
