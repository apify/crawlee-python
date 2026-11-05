from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from crawlee.autoscaling import AutoscaledPool
from crawlee.crawlers._basic._crawler_run import CrawlerRun
from tests._utils import poll_until_condition


@pytest.fixture
def run() -> CrawlerRun:
    return CrawlerRun(
        logger=Mock(),
        concurrency_system=Mock(),
        consumer=Mock(),
        run_task_function=AsyncMock(),
        is_task_ready_function=AsyncMock(),
        is_finished_function=AsyncMock(),
    )


async def test_earliest_ready_at_wins(run: CrawlerRun) -> None:
    """A scheduled wake-up is replaced only by an earlier `ready_at`."""
    now = datetime.now(UTC)

    with patch.object(AutoscaledPool, 'notify') as notify:
        run.schedule_wake(now + timedelta(hours=1))
        replaced_timer = run._wake_timer
        run.schedule_wake(now + timedelta(milliseconds=50))
        run.schedule_wake(now + timedelta(hours=2))

        assert await poll_until_condition(lambda: notify.called)

    assert replaced_timer is not None
    assert replaced_timer.cancelled()
    notify.assert_called_once()


async def test_finished_run_schedules_no_wake(run: CrawlerRun) -> None:
    """A run that has finished arms no wake-up."""
    run.finish()

    run.schedule_wake(datetime.now(UTC))

    assert run._wake_timer is None


async def test_past_ready_at_wakes_after_min_delay(run: CrawlerRun) -> None:
    """A `ready_at` that has already passed wakes the task loop after `_MIN_WAKE_DELAY`, not at once."""
    with patch.object(asyncio.get_running_loop(), 'call_later') as call_later:
        run.schedule_wake(datetime.now(UTC) - timedelta(seconds=1))

    call_later.assert_called_once_with(CrawlerRun._MIN_WAKE_DELAY.total_seconds(), run._wake)
