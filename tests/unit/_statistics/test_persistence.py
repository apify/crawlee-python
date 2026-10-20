from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from crawlee.statistics import Statistics
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    import pytest


async def test_basic_persistence() -> None:
    key = 'statistics_foo'

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        statistics.state.requests_failed = 42

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert statistics.state.requests_failed == 42


async def test_first_periodic_log_of_fresh_run_reports_zero_runtime(caplog: pytest.LogCaptureFixture) -> None:
    """The first periodic log of a fresh run reports a runtime of exactly zero."""
    caplog.set_level(logging.INFO)
    log_message = 'Fresh statistics'

    async with Statistics.with_default_state(log_message=log_message, statistics_log_format='inline'):
        pass

    periodic_records = [record for record in caplog.records if record.message == log_message]
    assert periodic_records
    assert periodic_records[0].crawler_runtime == 0  # ty: ignore[unresolved-attribute]


async def test_periodic_log_after_resume_excludes_downtime(caplog: pytest.LogCaptureFixture) -> None:
    """The first periodic log of a resumed run reports exactly the previous runtime, without the downtime."""
    caplog.set_level(logging.INFO)
    key = 'statistics_downtime_clean'
    log_message = 'Statistics after resume'
    downtime = timedelta(hours=2)

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key):
        pass

    # Simulate a resurrection after two hours of downtime by shifting the persisted timestamps into the past.
    kvs = await KeyValueStore.open()
    stored_state = await kvs.get_value(key)
    for field in ('crawlerStartedAt', 'crawlerLastStartTimestamp', 'crawlerFinishedAt', 'statsPersistedAt'):
        stored_state[field] = (datetime.fromisoformat(stored_state[field]) - downtime).isoformat()
    await kvs.set_value(key, stored_state)

    caplog.clear()
    async with Statistics.with_default_state(
        persistence_enabled=True,
        persist_state_key=key,
        log_message=log_message,
        statistics_log_format='inline',
    ):
        pass

    periodic_records = [record for record in caplog.records if record.message == log_message]
    assert periodic_records
    first_logged_runtime = timedelta(seconds=periodic_records[0].crawler_runtime)  # ty: ignore[unresolved-attribute]
    previous_runtime = timedelta(milliseconds=stored_state['crawlerRuntimeMillis'])
    assert abs(first_logged_runtime - previous_runtime) < timedelta(milliseconds=1)


async def test_runtime_accumulates_over_multiple_resurrections() -> None:
    """The persisted total runtime is restored on resume, so runs before the last one still count."""
    key = 'statistics_accumulated_runtime'
    now = datetime.now(UTC)
    last_run_start = now - timedelta(hours=1)
    last_run_duration = timedelta(seconds=5)
    total_runtime = timedelta(seconds=30)

    # State persisted after a clean run whose own segment took 5s, with 30s of runtime accumulated in total.
    kvs = await KeyValueStore.open()
    await kvs.set_value(
        key,
        {
            'crawlerStartedAt': (now - timedelta(hours=2)).isoformat(),
            'crawlerLastStartTimestamp': last_run_start.isoformat(),
            'crawlerFinishedAt': (last_run_start + last_run_duration).isoformat(),
            'statsPersistedAt': (last_run_start + last_run_duration).isoformat(),
            'crawlerRuntimeMillis': total_runtime.total_seconds() * 1000,
        },
    )

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        runtime = statistics.state.crawler_runtime

    assert total_runtime <= runtime < total_runtime + timedelta(minutes=1)


async def test_runtime_after_unclean_shutdown_excludes_downtime() -> None:
    """A state checkpointed mid-run restores the runtime up to the last persist, without the downtime that follows."""
    key = 'statistics_downtime_unclean'
    now = datetime.now(UTC)
    downtime = timedelta(hours=2)
    previous_runtime = timedelta(seconds=10)

    # State persisted by a periodic persist during a run that never finished cleanly (migration or abort):
    # there is no finish timestamp, only the runtime checkpointed at the last persist.
    kvs = await KeyValueStore.open()
    await kvs.set_value(
        key,
        {
            'crawlerStartedAt': (now - downtime - previous_runtime).isoformat(),
            'crawlerLastStartTimestamp': (now - downtime - previous_runtime).isoformat(),
            'crawlerFinishedAt': None,
            'statsPersistedAt': (now - downtime).isoformat(),
            'crawlerRuntimeMillis': previous_runtime.total_seconds() * 1000,
        },
    )

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        runtime = statistics.state.crawler_runtime

    assert previous_runtime <= runtime < previous_runtime + timedelta(minutes=1)


async def test_serialized_state_includes_runtime_of_run_in_progress() -> None:
    """A state serialized during a run carries the runtime up to that moment, so every persist is a checkpoint."""
    run_in_progress_duration = timedelta(hours=1)

    async with Statistics.with_default_state() as statistics:
        # Simulate a run that has been in progress for an hour by shifting its start timestamp into the past.
        assert statistics.state.crawler_last_started_at
        statistics.state.crawler_last_started_at -= run_in_progress_duration
        serialized = statistics.state.model_dump(by_alias=True)

    assert serialized['crawlerRuntimeMillis'] >= run_in_progress_duration.total_seconds() * 1000


async def test_backwards_clock_jump_does_not_regress_runtime() -> None:
    """A backwards wall clock jump during a run does not push the runtime below the accumulated total."""
    async with Statistics.with_default_state() as statistics:
        # Simulate a backwards clock jump by shifting the start timestamp of the run into the future.
        assert statistics.state.crawler_last_started_at
        statistics.state.crawler_last_started_at += timedelta(hours=1)
        runtime = statistics.state.crawler_runtime

    assert runtime == timedelta()


async def test_runtime_stops_growing_after_run_finishes() -> None:
    """The runtime measured by a finished run stays frozen instead of following the clock."""
    async with Statistics.with_default_state() as statistics:
        pass

    runtime_after_exit = statistics.state.crawler_runtime
    await asyncio.sleep(0.05)

    assert statistics.state.crawler_runtime == runtime_after_exit


async def test_runtime_carries_over_between_runs() -> None:
    """A second run continues accumulating the runtime from the total persisted by the first run."""
    key = 'statistics_restored_runtime'

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key):
        pass

    kvs = await KeyValueStore.open()
    stored_state = await kvs.get_value(key)
    previous_runtime = timedelta(milliseconds=stored_state['crawlerRuntimeMillis'])

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert previous_runtime <= statistics.state.crawler_runtime < previous_runtime + timedelta(minutes=1)
