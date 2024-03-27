# ruff: noqa: FBT003 Boolean positional value in function call

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Awaitable, TypeVar, cast
from unittest.mock import Mock

import pytest

from crawlee._utils.measure_time import measure_time
from crawlee.autoscaling.autoscaled_pool import AutoscaledPool
from crawlee.autoscaling.system_status import SystemStatus
from crawlee.autoscaling.types import LoadRatioInfo, SystemInfo


@pytest.fixture()
def system_status() -> SystemStatus | Mock:
    return Mock(spec=SystemStatus)


T = TypeVar('T')


def future(value: T, /) -> Awaitable[T]:
    f = asyncio.Future[T]()
    f.set_result(value)
    return f


async def test_runs_concurrently(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(done_count >= 10),
        min_concurrency=10,
        max_concurrency=10,
    )

    with measure_time() as elapsed:
        await pool.run()

    assert elapsed.wall is not None
    assert elapsed.wall < 0.3

    assert done_count >= 10


async def test_abort_works(system_status: SystemStatus | Mock) -> None:
    async def run() -> None:
        await asyncio.sleep(60)

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
        min_concurrency=10,
        max_concurrency=10,
    )

    with measure_time() as elapsed:
        run_task = asyncio.create_task(pool.run())
        await asyncio.sleep(0.1)
        assert pool.current_concurrency == 10
        await pool.abort()
        assert pool.current_concurrency == 0
        await run_task

    assert elapsed.wall is not None
    assert elapsed.wall < 0.3


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
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(done_count >= 20),
        min_concurrency=10,
        max_concurrency=10,
    )

    with pytest.raises(RuntimeError, match='Scheduled crash'):
        await pool.run()

    assert done_count < 20


async def test_propagates_exceptions_after_finished(system_status: SystemStatus | Mock) -> None:
    started_count = 0

    async def run() -> None:
        nonlocal started_count
        started_count += 1

        await asyncio.sleep(1)

        raise RuntimeError('Scheduled crash')

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(started_count > 0),
        min_concurrency=1,
        max_concurrency=1,
    )

    with pytest.raises(RuntimeError, match='Scheduled crash'):
        await pool.run()


async def test_autoscales(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    start = datetime.now(timezone.utc)

    def get_historical_system_info() -> SystemInfo:
        result = SystemInfo(
            cpu_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            memory_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            event_loop_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            client_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
        )

        # 0.5 seconds after the start of the test, pretend the CPU became overloaded
        if result.created_at - start >= timedelta(seconds=0.5):
            result.cpu_info = LoadRatioInfo(limit_ratio=0.9, actual_ratio=1.0)

        return result

    cast(Mock, system_status.get_historical_system_info).side_effect = get_historical_system_info

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
        min_concurrency=1,
        desired_concurrency=1,
        max_concurrency=4,
        autoscale_interval=timedelta(seconds=0.1),
    )

    pool_run_task = asyncio.create_task(pool.run(), name='pool run task')

    try:
        # After 0.2s, there should be an increase in concurrency
        await asyncio.sleep(0.2)
        assert pool.desired_concurrency > 1

        # After 0.5s, the concurrency should reach max concurrency
        await asyncio.sleep(0.3)
        assert pool.desired_concurrency == 4

        # The concurrency should guarantee completion of more than 10 tasks (a single worker would complete ~5)
        assert done_count > 10

        # After 0.7s, the pretend overload should have kicked in and there should be a drop in desired concurrency
        await asyncio.sleep(0.2)
        assert pool.desired_concurrency < 4

        # After a full second, the pool should scale down all the way to 1
        await asyncio.sleep(0.3)
        assert pool.desired_concurrency == 1
    finally:
        pool_run_task.cancel()
        with suppress(asyncio.CancelledError):
            await pool_run_task


async def test_max_tasks_per_minute_works(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
        min_concurrency=1,
        desired_concurrency=1,
        max_concurrency=1,
        max_tasks_per_minute=120,
    )

    pool_run_task = asyncio.create_task(pool.run(), name='pool run task')
    try:
        await asyncio.sleep(0.5)
        assert done_count <= 1
    finally:
        pool_run_task.cancel()
        with suppress(asyncio.CancelledError):
            await pool_run_task


async def test_allows_multiple_run_calls(system_status: SystemStatus | Mock) -> None:
    done_count = 0

    async def run() -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(done_count >= 4),
        min_concurrency=4,
        max_concurrency=4,
    )

    await pool.run()
    assert done_count == 4

    done_count = 0

    await pool.run()
    assert done_count == 4
