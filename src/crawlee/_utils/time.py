from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import timedelta

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
