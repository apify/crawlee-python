from __future__ import annotations

import math
from datetime import timedelta
from typing import Annotated, Any

from pydantic import PlainSerializer, PlainValidator


def timedelta_to_ms(td: timedelta | None) -> Any:
    if td == timedelta.max:
        return math.inf

    if td is None:
        return td

    return int(round(td.total_seconds() * 1000))


def timedelta_from_ms(value: float | timedelta | Any | None) -> Any:
    if isinstance(value, timedelta):
        return value

    if value == math.inf:
        return timedelta.max

    if not isinstance(value, (int, float)):
        return value

    return timedelta(milliseconds=value)


timedelta_ms = Annotated[timedelta, PlainSerializer(timedelta_to_ms), PlainValidator(timedelta_from_ms)]
