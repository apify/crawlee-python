from __future__ import annotations

from asyncio import sleep as _retry_sleep  # Using alias for testing purposes
from datetime import timedelta
from functools import wraps
from logging import getLogger
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

P = ParamSpec('P')
T = TypeVar('T')

logger = getLogger(__name__)


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
    if max_attempts < 1:
        raise ValueError('max_attempts must be at least 1')

    if base_delay < timedelta(0):
        raise ValueError('base_delay must be a non-negative timedelta')

    if not exception_types:
        raise ValueError('At least one exception type must be specified')

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:

        func_qualname = getattr(func, '__qualname__', repr(func))
        base_delay_seconds = base_delay.total_seconds()

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exception_types as exc:  # noqa: PERF203
                    if attempt >= max_attempts - 1:
                        raise

                    delay = base_delay_seconds * (2**attempt)
                    logger.debug(
                        f'{func_qualname}: attempt {attempt + 1}/{max_attempts} failed. '
                        f'Retrying in {delay:.2f} seconds.',
                        exc_info=exc,
                    )
                    await _retry_sleep(delay)
            raise RuntimeError('Unreachable')

        return wrapper

    return decorator
