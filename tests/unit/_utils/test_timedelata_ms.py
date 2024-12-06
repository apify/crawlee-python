from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from crawlee._utils.models import timedelta_ms


class _ModelWithTimedeltaMs(BaseModel):
    time_delta: timedelta_ms | None = None


@pytest.mark.parametrize(
    ('time_delta_input', 'expected_time_delta', 'expected_model_dump_value'),
    [
        (1.0, timedelta(milliseconds=1), 1),
        (1, timedelta(milliseconds=1), 1),
        ('1', timedelta(milliseconds=1), 1),
        (timedelta(milliseconds=1), timedelta(milliseconds=1), 1),
        (3.01, timedelta(microseconds=3010), 3),
        (3.5, timedelta(microseconds=3500), 4),
        (3.99, timedelta(microseconds=3990), 4),
        (None, None, None),
        (float('inf'), timedelta(days=999999999, seconds=3600 * 24 - 1, microseconds=999999), float('inf')),
    ],
)
def test_model_with_timedelta_ms_input_types(
    time_delta_input: float | timedelta | Any | None, expected_time_delta: timedelta, expected_model_dump_value: int
) -> None:
    model = _ModelWithTimedeltaMs(time_delta=time_delta_input)
    assert model.time_delta == expected_time_delta
    assert model.model_dump() == {'time_delta': expected_model_dump_value}
