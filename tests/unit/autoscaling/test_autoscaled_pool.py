from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, Mock

import pytest

from crawlee._utils.measure_time import measure_time
from crawlee.autoscaling.autoscaled_pool import AutoscaledPool
from crawlee.autoscaling.system_status import SystemStatus


@pytest.fixture()
def system_status() -> SystemStatus | Mock:
    return MagicMock(spec=SystemStatus)


@pytest.mark.asyncio()
async def test_runs_concurrently(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: True,
        is_finished_function=lambda: done_count >= 10,
        min_concurrency=10,
        max_concurrency=10,
    )

    with measure_time() as elapsed:
        await pool.run()

    assert elapsed.wall is not None
    assert elapsed.wall < 0.3

    assert done_count >= 10


@pytest.mark.asyncio()
async def test_propagates_exceptions(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

        if done_count > 5:
            raise RuntimeError('Scheduled crash')

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: True,
        is_finished_function=lambda: done_count >= 20,
        min_concurrency=10,
        max_concurrency=10,
    )

    with pytest.raises(RuntimeError):
        await pool.run()

    assert done_count < 20
