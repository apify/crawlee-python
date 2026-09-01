from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
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
    """The first periodic log of a fresh run must report a runtime of exactly zero."""
    caplog.set_level(logging.INFO)
    log_message = 'Fresh statistics'

    async with Statistics.with_default_state(log_message=log_message, statistics_log_format='inline'):
        pass

    periodic_records = [record for record in caplog.records if record.message == log_message]
    assert periodic_records
    assert periodic_records[0].crawler_runtime == 0  # ty: ignore[unresolved-attribute]


async def test_periodic_log_after_resume_excludes_downtime(caplog: pytest.LogCaptureFixture) -> None:
    """The first periodic log of a resumed run must report only the previous runtime, without the downtime."""
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
        # `datetime.fromisoformat` does not accept the 'Z' suffix until Python 3.11.
        stored_timestamp = datetime.fromisoformat(stored_state[field].replace('Z', '+00:00'))
        stored_state[field] = (stored_timestamp - downtime).isoformat()
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
    """The persisted total runtime is restored as the runtime offset, so runs before the last one still count."""
    key = 'statistics_accumulated_runtime'
    now = datetime.now(timezone.utc)
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
    """State persisted mid-run (migration, abort): the runtime of the previous run is approximated by the moment
    the state was last persisted, so the downtime before the resumed run must not inflate the runtime."""
    key = 'statistics_downtime_unclean'
    now = datetime.now(timezone.utc)
    downtime = timedelta(hours=2)
    previous_runtime = timedelta(seconds=10)

    kvs = await KeyValueStore.open()
    await kvs.set_value(
        key,
        {
            'requestsFinished': 2,
            'crawlerStartedAt': (now - downtime - previous_runtime).isoformat(),
            'crawlerLastStartTimestamp': (now - downtime - previous_runtime).isoformat(),
            'crawlerFinishedAt': None,
            'statsPersistedAt': (now - downtime).isoformat(),
        },
    )

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        runtime = statistics.state.crawler_runtime

    assert previous_runtime <= runtime < previous_runtime + downtime
