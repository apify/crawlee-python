from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextvars import Context, Token
    from types import TracebackType

    _TimeoutExtender = Callable[[timedelta], bool]

_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3600

_active_timeout_extenders: ContextVar[tuple[_TimeoutExtender, ...]] = ContextVar('active_timeout_extenders', default=())
"""Extenders of the `SharedTimeout`s entered in the current context, from the outermost to the innermost."""


@dataclass
class TimerResult:
    wall: float | None = None
    cpu: float | None = None


@contextmanager
def measure_time() -> Generator[TimerResult]:
    """Measure the execution time (wall-clock and CPU) between the start and end of the with-block."""
    result = TimerResult()
    before_wall = time.monotonic()
    before_cpu = time.thread_time()

    try:
        yield result
    finally:
        after_wall = time.monotonic()
        after_cpu = time.thread_time()
        result.wall = after_wall - before_wall
        result.cpu = after_cpu - before_cpu


class SharedTimeout:
    """Keeps track of a time budget shared by multiple independent async operations.

    Provides a reusable, non-reentrant context manager interface. While entered, `extend_active_timeouts` can extend it.
    """

    def __init__(self, timeout: timedelta) -> None:
        self._remaining_timeout = timeout
        self._active_timeout: asyncio.Timeout | None = None
        self._activation_timestamp: float | None = None
        self._extenders_token: Token[tuple[_TimeoutExtender, ...]] | None = None

    async def __aenter__(self) -> timedelta:
        if self._active_timeout is not None or self._activation_timestamp is not None:
            raise RuntimeError('A shared timeout context cannot be entered twice at the same time')

        self._activation_timestamp = time.monotonic()
        self._active_timeout = new_timeout = asyncio.timeout(self._remaining_timeout.total_seconds())
        await new_timeout.__aenter__()

        # Entering and exiting must run in the same context. Otherwise, resetting the token fails.
        extender = partial(self._extend, new_timeout, asyncio.get_running_loop())
        self._extenders_token = _active_timeout_extenders.set((*_active_timeout_extenders.get(), extender))
        return self._remaining_timeout

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        if self._active_timeout is None or self._activation_timestamp is None or self._extenders_token is None:
            raise RuntimeError('Logic error')

        try:
            await self._active_timeout.__aexit__(exc_type, exc_value, exc_traceback)
        finally:
            elapsed = time.monotonic() - self._activation_timestamp
            self._remaining_timeout = self._remaining_timeout - timedelta(seconds=elapsed)

            extenders_token = self._extenders_token
            self._active_timeout = None
            self._activation_timestamp = None
            self._extenders_token = None
            _active_timeout_extenders.reset(extenders_token)

    def _extend(self, entered_timeout: asyncio.Timeout, loop: asyncio.AbstractEventLoop, delta: timedelta) -> bool:
        """Extend the budget by `delta` and return whether the enclosing timeouts should be extended as well."""
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is not loop:
            raise RuntimeError('A timeout can only be extended from the event loop it runs on, not from other threads.')

        # The timeout has been exited since, so only its later uses get the extra time.
        if entered_timeout is not self._active_timeout:
            self._remaining_timeout += delta
            return False

        if entered_timeout.expired():
            return False

        deadline = entered_timeout.when()
        if deadline is not None:
            entered_timeout.reschedule(deadline + delta.total_seconds())

        self._remaining_timeout += delta
        return True


def extend_active_timeouts(delta: timedelta) -> None:
    """Extend the `SharedTimeout`s entered in the current context by `delta`, from the innermost outwards.

    Stops at a timeout that is no longer running, so the ones that still bound the caller stay as they are.
    """
    for extend in reversed(_active_timeout_extenders.get()):
        if not extend(delta):
            return


def context_without_active_timeouts() -> Context:
    """Copy the current context without the entered `SharedTimeout`s, so the code running in it can't extend them."""
    context = copy_context()
    context.run(_active_timeout_extenders.set, ())
    return context


def format_duration(duration: timedelta | None) -> str:
    """Format a timedelta into a human-readable string with appropriate units."""
    if duration is None:
        return 'None'

    total_seconds = duration.total_seconds()

    if total_seconds == 0:
        return '0s'

    # For very small durations, show in milliseconds
    if total_seconds < 1:
        milliseconds = total_seconds * 1000
        if milliseconds < 1:
            microseconds = total_seconds * 1_000_000
            return f'{microseconds:.1f}μs'
        return f'{milliseconds:.1f}ms'

    # For durations less than 60 seconds, show in seconds
    if total_seconds < _SECONDS_PER_MINUTE:
        return f'{total_seconds:.2f}s'

    # For durations less than 1 hour, show in minutes and seconds
    if total_seconds < _SECONDS_PER_HOUR:
        minutes = int(total_seconds // _SECONDS_PER_MINUTE)
        seconds = total_seconds % _SECONDS_PER_MINUTE
        if seconds == 0:
            return f'{minutes}min'
        return f'{minutes}min {seconds:.1f}s'

    # For longer durations, show in hours, minutes, and seconds
    hours = int(total_seconds // _SECONDS_PER_HOUR)
    remaining_seconds = total_seconds % _SECONDS_PER_HOUR
    minutes = int(remaining_seconds // _SECONDS_PER_MINUTE)
    seconds = remaining_seconds % _SECONDS_PER_MINUTE

    result = f'{hours}h'
    if minutes > 0 or seconds > 0:
        result += f' {minutes}min'
    if seconds > 0:
        result += f' {seconds:.1f}s'

    return result
