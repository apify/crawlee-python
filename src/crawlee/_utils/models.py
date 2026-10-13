from __future__ import annotations

from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, ConfigDict, PlainSerializer, TypeAdapter, ValidationError, WrapValidator
from pydantic.alias_generators import to_camel

if TYPE_CHECKING:
    from collections.abc import Callable

"""Utility types for Pydantic models."""


def _timedelta_to_ms(td: timedelta | None) -> float | None:
    if td == timedelta.max:
        return float('inf')
    if td is None:
        return td
    return round(td.total_seconds() * 1000)


def _timedelta_to_secs(td: timedelta | None) -> float | None:
    if td == timedelta.max:
        return float('inf')
    if td is None:
        return td
    return td.total_seconds()


_number_parser = TypeAdapter(float)


def _timedelta_from_ms(value: float | timedelta | Any | None, handler: Callable[[Any], timedelta]) -> timedelta | None:
    if value == float('inf'):
        return timedelta.max

    # If the value is a string-encoded number, decode it
    if isinstance(value, str):
        with suppress(ValidationError):
            value = _number_parser.validate_python(value)

    if not isinstance(value, (int, float)):
        return handler(value)

    return timedelta(milliseconds=value)


def _timedelta_from_secs(
    value: float | timedelta | Any | None,
    handler: Callable[[Any], timedelta],
) -> timedelta | None:
    if value == float('inf'):
        return timedelta.max

    # If the value is a string-encoded number, decode it
    if isinstance(value, str):
        with suppress(ValidationError):
            value = _number_parser.validate_python(value)

    if not isinstance(value, (int, float)):
        return handler(value)

    return timedelta(seconds=value)


timedelta_ms = Annotated[timedelta, PlainSerializer(_timedelta_to_ms), WrapValidator(_timedelta_from_ms)]
timedelta_secs = Annotated[timedelta, PlainSerializer(_timedelta_to_secs), WrapValidator(_timedelta_from_secs)]


class CamelCaseModel(BaseModel):
    """Base model whose fields (de)serialize under camelCase aliases generated from their names.

    A field whose external key differs from the generated one sets an explicit `Field(alias=...)`.
    """

    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True, validate_by_alias=True)
