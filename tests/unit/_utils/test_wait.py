from __future__ import annotations

import asyncio

import pytest

from crawlee._utils.wait import run_all, task_group


async def test_run_all_returns_results_in_order() -> None:
    """Results come back in the order of the coroutines, not in the order they finish."""

    async def delayed(value: int) -> int:
        await asyncio.sleep(0.01 * value)
        return value

    assert await run_all(delayed(3), delayed(1), delayed(2)) == [3, 1, 2]


async def test_run_all_raises_first_failure_unwrapped_and_cancels_the_rest() -> None:
    """A failing coroutine's own exception is raised, and the coroutines still running are cancelled."""
    sibling_cancelled = asyncio.Event()

    async def fail() -> None:
        await asyncio.sleep(0.01)
        raise ValueError('boom')

    async def run_forever() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    with pytest.raises(ValueError, match='boom') as exc_info:
        await run_all(run_forever(), fail())

    assert sibling_cancelled.is_set()
    assert exc_info.value.__context__ is None


async def test_task_group_raises_body_failure_unwrapped() -> None:
    """An exception from the body of `task_group` is raised as is and cancels the tasks in the group."""
    task_cancelled = asyncio.Event()

    async def run_forever() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    async def fail_in_body() -> None:
        async with task_group() as tg:
            tg.create_task(run_forever())
            await asyncio.sleep(0)
            raise KeyError('body')

    with pytest.raises(KeyError):
        await fail_in_body()

    assert task_cancelled.is_set()


async def test_task_group_propagates_outer_cancellation() -> None:
    """Cancelling the task that awaits `task_group` raises `CancelledError`, not an exception group."""

    async def wait_in_group() -> None:
        async with task_group() as tg:
            tg.create_task(asyncio.sleep(10))

    outer = asyncio.create_task(wait_in_group())
    await asyncio.sleep(0.01)
    outer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer
