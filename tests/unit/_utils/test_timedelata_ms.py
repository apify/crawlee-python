from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from crawlee._utils.models import timedelta_ms


class _ModelWithTimedeltaMs(BaseModel):
    time_delta: timedelta_ms | None = None


@pytest.mark.parametrize('time_delta_input', [1.0, 1, '1', timedelta(milliseconds=1)])
def test_model_with_timedelta_ms_input_types(time_delta_input: float | timedelta | Any | None) -> None:
    model = _ModelWithTimedeltaMs(time_delta=time_delta_input)
    assert model.time_delta == timedelta(milliseconds=1)
    assert model.model_dump() == {'time_delta': 1}


def test_model_with_timedelta_ms_none() -> None:
    model = _ModelWithTimedeltaMs(time_delta=None)
    assert model.model_dump() == {'time_delta': None}
