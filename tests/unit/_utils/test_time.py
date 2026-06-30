from __future__ import annotations

from datetime import timedelta

import pytest

from crawlee._utils.time import format_duration


@pytest.mark.parametrize(
    ('duration', 'expected'),
    [
        (None, 'None'),
        (timedelta(seconds=0), '0s'),
        (timedelta(microseconds=500), '500.0μs'),
        (timedelta(milliseconds=500), '500.0ms'),
        (timedelta(seconds=59.5), '59.50s'),
        (timedelta(seconds=90), '1min 30.0s'),
        (timedelta(minutes=2), '2min'),
        (timedelta(hours=2), '2h'),
        (timedelta(hours=2, minutes=1), '2h 1min'),
        (timedelta(hours=2, minutes=1, seconds=30), '2h 1min 30.0s'),
        # Hours with no full minutes but leftover seconds must keep the minutes
        # segment, otherwise '1h 30.0s' misleadingly reads as 1h30m.
        (timedelta(hours=1, seconds=30), '1h 0min 30.0s'),
        (timedelta(hours=1, seconds=1), '1h 0min 1.0s'),
    ],
)
def test_format_duration(duration: timedelta | None, expected: str) -> None:
    assert format_duration(duration) == expected
