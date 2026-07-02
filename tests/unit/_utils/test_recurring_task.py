from __future__ import annotations

import asyncio
import logging
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


@pytest.mark.run_alone
async def test_execution(function: AsyncMock, delay: timedelta) -> None:
    task = RecurringTask(function, delay)

    task.start()
    await asyncio.sleep(0.2)  # Wait enough for the task to execute a few times
    await task.stop()

    assert isinstance(task.func, AsyncMock)  # To let type checker know that the function is a mock
    assert task.func.call_count >= 3

    await task.stop()


async def test_execution_continues_after_exception(delay: timedelta, caplog: pytest.LogCaptureFixture) -> None:
    """Test that the recurring task logs an exception raised by its function and keeps executing."""
    caplog.set_level(logging.ERROR, logger='crawlee._utils.recurring_task')
    call_count = 0
    second_call_done = asyncio.Event()

    async def func() -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError('Scheduled crash')
        second_call_done.set()

    task = RecurringTask(func, delay)
    task.start()

    await asyncio.wait_for(second_call_done.wait(), timeout=5)
    await task.stop()

    assert call_count >= 2
    assert any(
        record.name == 'crawlee._utils.recurring_task' and record.levelno == logging.ERROR and record.exc_info
        for record in caplog.records
    )
