# ruff: noqa: TCH002 TCH003

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any

from dateutil import parser
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SessionModel(BaseModel):
    """Model for a Session object."""

    model_config = ConfigDict(populate_by_name=True)

    id: Annotated[str, Field(alias='id')]
    max_age: Annotated[timedelta, Field(alias='maxAge')]
    user_data: Annotated[dict, Field(alias='userData')]
    max_error_score: Annotated[float, Field(alias='maxErrorScore')]
    error_score_decrement: Annotated[float, Field(alias='errorScoreDecrement')]
    created_at: Annotated[datetime, Field(alias='createdAt')]
    usage_count: Annotated[int, Field(alias='usageCount')]
    max_usage_count: Annotated[int, Field(alias='maxUsageCount')]
    error_score: Annotated[float, Field(alias='errorScore')]
    cookies: Annotated[dict, Field(alias='cookies')]
    blocked_status_codes: Annotated[list[int], Field(alias='blockedStatusCodes')]

    @field_validator('max_age', mode='before')
    @classmethod
    def parse_max_age(cls, value: Any) -> timedelta:
        """Try to parse max_age field into a timedelta object."""
        if isinstance(value, timedelta):
            return value

        if isinstance(value, str):
            try:
                parsed_time = parser.parse(value)
                return timedelta(hours=parsed_time.hour, minutes=parsed_time.minute, seconds=parsed_time.second)
            except ValueError as exc:
                raise ValueError(f"Invalid time format for max_age. Expected 'HH:MM:SS', got {value}") from exc

        raise ValueError('Invalid data type for max_age')


class SessionPoolModel(BaseModel):
    """Model for a SessionPool object."""

    model_config = ConfigDict(populate_by_name=True)

    persistence_enabled: Annotated[bool, Field(alias='persistenceEnabled')]
    persist_state_kvs_name: Annotated[str, Field(alias='persistStateKvsName')]
    persist_state_key: Annotated[str, Field(alias='persistStateKey')]
    max_pool_size: Annotated[int, Field(alias='maxPoolSize')]
    session_count: Annotated[int, Field(alias='sessionCount')]
    usable_session_count: Annotated[int, Field(alias='usableSessionCount')]
    retired_session_count: Annotated[int, Field(alias='retiredSessionCount')]
    sessions: Annotated[list[SessionModel], Field(alias='sessions')]
