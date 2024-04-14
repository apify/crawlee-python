# ruff: noqa: TCH002 TCH003

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dateutil import parser
from pydantic import BaseModel, field_validator


class SessionModel(BaseModel):
    """Model for a Session object."""

    id: str
    max_age: timedelta
    user_data: dict
    max_error_score: float
    error_score_decrement: float
    created_at: datetime
    usage_count: int
    max_usage_count: int
    error_score: float
    cookies: dict
    blocked_status_codes: list[int]

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

    @field_validator('created_at', mode='before')
    @classmethod
    def parse_created_at(cls, value: Any) -> datetime:
        """Try to parse `created_at` field into a datetime object."""
        if isinstance(value, str):
            return datetime.fromisoformat(value)

        if isinstance(value, datetime):
            return value

        raise ValueError('Invalid data type for created_at')


class SessionPoolModel(BaseModel):
    """Model for a SessionPool object."""

    persistance_enabled: bool
    persist_state_kvs_name: str
    persist_state_key: str
    max_pool_size: int
    session_count: int
    usable_session_count: int
    retired_session_count: int
    sessions: list[SessionModel]

    @field_validator('sessions', mode='before')
    @classmethod
    def parse_sessions(cls, value: Any) -> list[SessionModel]:
        """Try to parse `sessions` field into a list of SessionModel objects."""
        if isinstance(value, list) and all(isinstance(item, SessionModel) for item in value):
            return value

        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return [SessionModel(**session) for session in value]

        raise ValueError('Invalid data type for created_at')
