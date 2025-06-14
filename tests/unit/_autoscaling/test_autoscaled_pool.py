# ruff: noqa: FBT003 # Boolean positional value in function call

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from itertools import chain, repeat
from typing import TYPE_CHECKING, TypeVar, cast
from unittest.mock import Mock

import pytest

from crawlee._autoscaling import AutoscaledPool, SystemStatus
from crawlee._autoscaling._types import LoadRatioInfo, SystemInfo
from crawlee._types import ConcurrencySettings
from crawlee._utils.measure_time import measure_time

if TYPE_CHECKING:
    from collections.abc import Awaitable


@pytest.fixture
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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=10,
            max_concurrency=10,
        ),
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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=10,
            max_concurrency=10,
        ),
    )

    with measure_time() as elapsed:
        run_task = asyncio.create_task(pool.run(), name='pool run task')
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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=10,
            max_concurrency=10,
        ),
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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=1,
            max_concurrency=1,
        ),
    )

    with pytest.raises(RuntimeError, match='Scheduled crash'):
        await pool.run()


async def test_autoscales(
    monkeypatch: pytest.MonkeyPatch,
    system_status: SystemStatus | Mock,
) -> None:
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

    cast('Mock', system_status.get_historical_system_info).side_effect = get_historical_system_info

    # Override AP class attributes using monkeypatch.
    monkeypatch.setattr(AutoscaledPool, '_AUTOSCALE_INTERVAL', timedelta(seconds=0.1))

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
        concurrency_settings=ConcurrencySettings(
            min_concurrency=1,
            desired_concurrency=1,
            max_concurrency=4,
        ),
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


async def test_autoscales_uses_desired_concurrency_ratio(
    monkeypatch: pytest.MonkeyPatch,
    system_status: SystemStatus | Mock,
) -> None:
    """Test that desired concurrency ratio can limit desired concurrency.

    This test creates situation where only one task is ready and then no other task is ever ready.
    This creates situation where the system could scale up desired concurrency, but it will not do so because
    desired_concurrency_ratio=1 means that first the system would have to increase current concurrency to same number as
    desired concurrency and due to no other task ever being ready, it will never happen. Thus desired concurrency will
    stay 2 as was the initial setup, even though other conditions would allow the increase. (max_concurrency=4,
    system being idle).
    """

    async def run() -> None:
        await asyncio.sleep(0.1)

    is_task_ready_iterator = chain([future(True)], repeat(future(False)))

    def is_task_ready_function() -> Awaitable[bool]:
        return next(is_task_ready_iterator)

    def get_historical_system_info() -> SystemInfo:
        return SystemInfo(
            cpu_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            memory_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            event_loop_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
            client_info=LoadRatioInfo(limit_ratio=0.9, actual_ratio=0.3),
        )

    cast('Mock', system_status.get_historical_system_info).side_effect = get_historical_system_info

    # Override AP class attributes using monkeypatch.
    monkeypatch.setattr(AutoscaledPool, '_AUTOSCALE_INTERVAL', timedelta(seconds=0.1))
    monkeypatch.setattr(AutoscaledPool, '_DESIRED_CONCURRENCY_RATIO', 1)

    pool = AutoscaledPool(
        system_status=system_status,
        run_task_function=run,
        is_task_ready_function=is_task_ready_function,
        is_finished_function=lambda: future(False),
        concurrency_settings=ConcurrencySettings(
            min_concurrency=2,
            desired_concurrency=2,
            max_concurrency=4,
        ),
    )

    pool_run_task = asyncio.create_task(pool.run(), name='pool run task')
    try:
        for _ in range(5):
            assert pool.desired_concurrency == 2
            await asyncio.sleep(0.1)

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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=1,
            desired_concurrency=1,
            max_concurrency=1,
            max_tasks_per_minute=120,
        ),
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
        concurrency_settings=ConcurrencySettings(
            min_concurrency=4,
            max_concurrency=4,
        ),
    )

    await pool.run()
    assert done_count == 4

    done_count = 0
    await asyncio.sleep(0.2)  # Allow any lingering callbacks to complete
    done_count = 0  # Reset again to ensure clean state

    await pool.run()
    assert done_count == 4
