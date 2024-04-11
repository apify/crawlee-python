from __future__ import annotations

import asyncio
import time

from crawlee._utils.measure_time import measure_time


def test_measure_time_wall_sync() -> None:
    with measure_time() as elapsed:
        time.sleep(0.1)

    assert elapsed.cpu is not None
    assert elapsed.wall is not None
    assert elapsed.wall >= 0.09


def test_measure_time_cpu_sync() -> None:
    with measure_time() as elapsed:
        start = time.time()
        acc = 0

        while time.time() - start < 0.1:
            acc += 1
            acc *= acc

    assert elapsed.cpu is not None
    assert elapsed.wall is not None
    assert elapsed.cpu >= 0.05


async def test_measure_time_wall_async() -> None:
    with measure_time() as elapsed:
        await asyncio.sleep(0.1)

    assert elapsed.cpu is not None
    assert elapsed.wall is not None
    assert elapsed.wall >= 0.09
