from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator


class TimerResult:
    wall: float | None
    cpu: float | None


@contextmanager
def measure_time() -> Iterator[TimerResult]:
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
