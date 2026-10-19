from __future__ import annotations

import pytest

from crawlee.statistics import Statistics, StatisticsState
from crawlee.storages import KeyValueStore


async def test_basic_persistence() -> None:
    key = 'statistics_foo'

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        statistics.state.requests_failed = 42

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert statistics.state.requests_failed == 42


@pytest.mark.parametrize(
    ('second_id', 'expected_requests_failed'),
    [
        pytest.param('a', 42, id='same id'),
        pytest.param('b', 0, id='different id'),
    ],
)
async def test_persistence_by_id(second_id: str, expected_requests_failed: int) -> None:
    """Statistics with the same ID share the persisted state, and statistics with different IDs don't."""
    async with Statistics.with_default_state(persistence_enabled=True, id='a') as statistics:
        statistics.state.requests_failed = 42

    async with Statistics.with_default_state(persistence_enabled=True, id=second_id) as statistics:
        pass

    assert statistics.state.requests_failed == expected_requests_failed


async def test_persistence_loads_numeric_id() -> None:
    """A persisted state whose `statsId` is a number loads with a string ID."""
    key = 'statistics_foo'
    kvs = await KeyValueStore.open()
    await kvs.set_value(key, {'statsId': 0, 'requestsFailed': 42})

    async with Statistics.with_default_state(persistence_enabled=True, persist_state_key=key) as statistics:
        pass

    assert statistics.state.stats_id == '0'
    assert statistics.state.requests_failed == 42


def test_id_defaults_to_counter() -> None:
    """Statistics without an ID take the next value of a counter that explicit IDs don't advance."""
    assert Statistics.with_default_state()._id == '0'
    assert Statistics.with_default_state(id='custom')._id == 'custom'
    assert Statistics.with_default_state()._id == '1'


def test_replace_state_model_keeps_id() -> None:
    """`replace_state_model` keeps the ID of the original statistics."""
    statistics = Statistics.with_default_state(id='custom')

    assert statistics.replace_state_model(StatisticsState)._id == 'custom'
