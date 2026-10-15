from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from crawlee.request_loaders import FinishedStatus, ReadyStatus, RequestSourceStatus, StalledStatus, WaitingStatus
from crawlee.request_loaders._utils import join_request_source_statuses

READY = ReadyStatus()
STALLED = StalledStatus('example.com rate-limited every request')
WAITING = WaitingStatus()
FINISHED = FinishedStatus()

EARLY = datetime(2026, 1, 1, tzinfo=UTC)
LATE = EARLY + timedelta(seconds=10)


@pytest.mark.parametrize(
    ('first_status', 'second_status', 'expected'),
    [
        pytest.param(READY, READY, READY, id='ready+ready'),
        pytest.param(READY, STALLED, READY, id='ready+stalled'),
        pytest.param(READY, WAITING, READY, id='ready+waiting'),
        pytest.param(READY, FINISHED, READY, id='ready+finished'),
        pytest.param(STALLED, STALLED, STALLED, id='stalled+stalled'),
        pytest.param(WAITING, STALLED, WAITING, id='waiting+stalled'),
        pytest.param(STALLED, FINISHED, STALLED, id='stalled+finished'),
        pytest.param(WAITING, WAITING, WAITING, id='waiting+waiting'),
        pytest.param(WAITING, FINISHED, WAITING, id='waiting+finished'),
        pytest.param(FINISHED, FINISHED, FINISHED, id='finished+finished'),
    ],
)
def test_join_precedence(
    first_status: RequestSourceStatus,
    second_status: RequestSourceStatus,
    expected: RequestSourceStatus,
) -> None:
    """Test that the join ranks ready over waiting over stalled over finished, in either order."""
    assert join_request_source_statuses(first_status, second_status) == expected
    assert join_request_source_statuses(second_status, first_status) == expected


def test_join_keeps_earliest_ready_at() -> None:
    """Test that the earlier of two known wake-up times wins."""
    early = WaitingStatus(ready_at=EARLY)
    late = WaitingStatus(ready_at=LATE)

    assert join_request_source_statuses(early, late) == early
    assert join_request_source_statuses(late, early) == early


def test_join_keeps_known_ready_at() -> None:
    """Test that a known wake-up time survives a source that has none."""
    known = WaitingStatus(ready_at=EARLY)

    assert join_request_source_statuses(WAITING, known) == known
    assert join_request_source_statuses(known, WAITING) == known
    assert join_request_source_statuses(known, FINISHED) == known
    assert join_request_source_statuses(STALLED, known) == known
