from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator


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
