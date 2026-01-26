from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from async_timeout import Timeout, timeout

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3600


@dataclass
class TimerResult:
    wall: float | None = None
    cpu: float | None = None


@contextmanager
def measure_time() -> Iterator[TimerResult]:
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

    Provides a reusable, non-reentrant context manager interface.
    """

    def __init__(self, timeout: timedelta) -> None:
        self._remaining_timeout = timeout
        self._active_timeout: Timeout | None = None
        self._activation_timestamp: float | None = None

    async def __aenter__(self) -> timedelta:
        if self._active_timeout is not None or self._activation_timestamp is not None:
            raise RuntimeError('A shared timeout context cannot be entered twice at the same time')

        self._activation_timestamp = time.monotonic()
        self._active_timeout = new_timeout = timeout(self._remaining_timeout.total_seconds())
        await new_timeout.__aenter__()
        return self._remaining_timeout

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        if self._active_timeout is None or self._activation_timestamp is None:
            raise RuntimeError('Logic error')

        await self._active_timeout.__aexit__(exc_type, exc_value, exc_traceback)
        elapsed = time.monotonic() - self._activation_timestamp
        self._remaining_timeout = self._remaining_timeout - timedelta(seconds=elapsed)

        self._active_timeout = None
        self._activation_timestamp = None


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
    if minutes > 0:
        result += f' {minutes}min'
    if seconds > 0:
        result += f' {seconds:.1f}s'

    return result
