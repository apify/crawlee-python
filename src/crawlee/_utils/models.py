from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Any, Callable

from pydantic import PlainSerializer, WrapValidator

"""Utility types for Pydantic models."""


def _timedelta_to_ms(td: timedelta | None) -> Any:
    if td == timedelta.max:
        return float('inf')

    if td is None:
        return td

    return int(round(td.total_seconds() * 1000))


def _timedelta_from_ms(value: float | timedelta | Any | None, handler: Callable[[Any], Any]) -> Any:
    if value == float('inf'):
        return timedelta.max

    if not isinstance(value, (int, float)):
        return handler(value)

    return timedelta(milliseconds=value)


timedelta_ms = Annotated[timedelta, PlainSerializer(_timedelta_to_ms), WrapValidator(_timedelta_from_ms)]
