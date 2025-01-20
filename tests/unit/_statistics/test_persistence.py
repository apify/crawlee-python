from __future__ import annotations

from crawlee.statistics import Statistics


async def test_basic_persistence() -> None:
    key = 'statistics_foo'

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        statistics.state.requests_failed = 42

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert statistics.state.requests_failed == 42
