from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from crawlee.autoscaling import LoadSnapshot, SnapshotStore


def snapshot(created_at: datetime) -> LoadSnapshot:
    """Build a snapshot of a resource that is not overloaded."""
    return LoadSnapshot(is_overloaded=False, created_at=created_at)


def test_store_keeps_time_order() -> None:
    """Snapshots arriving out of order are kept ordered by their creation time."""
    now = datetime.now(UTC)
    store = SnapshotStore[LoadSnapshot]()

    for delta in [3, 0, 5, 1, 6, 2, 4]:
        store.push(snapshot(now - timedelta(seconds=delta)))

    created_at = [item.created_at for item in store.get_all()]
    assert created_at == [now - timedelta(seconds=delta) for delta in range(6, -1, -1)]


def test_store_prunes_outside_window() -> None:
    """Only the snapshots within the sample window of the latest one are kept."""
    now = datetime.now(UTC)
    store = SnapshotStore[LoadSnapshot]()
    store.use_sample_window(timedelta(hours=2))

    for delta in [0, 3, 2, 5]:
        store.push(snapshot(now - timedelta(hours=delta)))

    assert [item.created_at for item in store.get_all()] == [now - timedelta(hours=2), now]


def test_store_keeps_all_without_window() -> None:
    """A store that was never given a sample window prunes nothing."""
    now = datetime.now(UTC)
    store = SnapshotStore[LoadSnapshot]()

    for delta in [0, 30, 60]:
        store.push(snapshot(now - timedelta(days=delta)))

    assert len(store.get_all()) == 3


@pytest.mark.parametrize(
    ('duration', 'expected_count'),
    [
        pytest.param(None, 5, id='no duration'),
        pytest.param(timedelta(hours=0.5), 1, id='latest only'),
        pytest.param(timedelta(hours=2.5), 3, id='part of history'),
        pytest.param(timedelta(hours=10), 5, id='longer than history'),
    ],
)
def test_store_sample_duration(duration: timedelta | None, expected_count: int) -> None:
    """A sample covers the snapshots within the duration of the latest one."""
    now = datetime.now(UTC)
    store = SnapshotStore[LoadSnapshot]()

    for delta in range(5, 0, -1):
        store.push(snapshot(now - timedelta(hours=delta)))

    assert len(store.get_sample(duration)) == expected_count


def test_store_clear() -> None:
    """Clearing the store drops every snapshot."""
    store = SnapshotStore[LoadSnapshot]()
    store.push(snapshot(datetime.now(UTC)))

    store.clear()

    assert store.get_sample() == []
