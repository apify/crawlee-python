from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from ._cookies import CookieParam


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
    cookies: Annotated[list[CookieParam], Field(alias='cookies')]
    blocked_status_codes: Annotated[list[int], Field(alias='blockedStatusCodes')]


class SessionPoolModel(BaseModel):
    """Model for a SessionPool object."""

    model_config = ConfigDict(populate_by_name=True)

    persistence_enabled: Annotated[bool, Field(alias='persistenceEnabled')]
    persist_state_kvs_name: Annotated[str | None, Field(alias='persistStateKvsName')]
    persist_state_key: Annotated[str, Field(alias='persistStateKey')]
    max_pool_size: Annotated[int, Field(alias='maxPoolSize')]
    session_count: Annotated[int, Field(alias='sessionCount')]
    usable_session_count: Annotated[int, Field(alias='usableSessionCount')]
    retired_session_count: Annotated[int, Field(alias='retiredSessionCount')]
    sessions: Annotated[list[SessionModel], Field(alias='sessions')]
