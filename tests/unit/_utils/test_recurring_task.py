from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from crawlee._utils.recurring_task import RecurringTask


@pytest.fixture
def function() -> AsyncMock:
    mock_function = AsyncMock()
    mock_function.__name__ = 'mocked_function'  # To avoid issues with the function name in RecurringTask
    return mock_function


@pytest.fixture
def delay() -> timedelta:
    return timedelta(milliseconds=30)


async def test_init(function: AsyncMock, delay: timedelta) -> None:
    rt = RecurringTask(function, delay)
    assert rt.func == function
    assert rt.delay == delay
    assert rt.task is None


async def test_start_and_stop(function: AsyncMock, delay: timedelta) -> None:
    rt = RecurringTask(function, delay)

    rt.start()
    await asyncio.sleep(0)  # Yield control to allow the task to start

    assert isinstance(rt.task, asyncio.Task)
    assert not rt.task.done()

    await rt.stop()
    assert rt.task.done()


async def test_execution(function: AsyncMock, delay: timedelta) -> None:
    task = RecurringTask(function, delay)

    task.start()
    await asyncio.sleep(0.1)  # Wait enough for the task to execute a few times
    await task.stop()

    assert isinstance(task.func, AsyncMock)  # To let MyPy know that the function is a mocked
    assert task.func.call_count >= 3

    await task.stop()
