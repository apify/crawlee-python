from crawlee.statistics.statistics import Statistics


async def test_basic_persistence() -> None:
    key = 'statistics_foo'

    async with Statistics(persistence_enabled=True, persist_state_key=key) as statistics:
        statistics.state.requests_failed = 42

    async with Statistics(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert statistics.state.requests_failed == 42
