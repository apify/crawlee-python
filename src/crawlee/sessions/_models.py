from __future__ import annotations

from collections.abc import MutableMapping
from datetime import datetime, timedelta
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    GetPydanticSchema,
    PlainSerializer,
    computed_field,
)
from pydantic.alias_generators import to_camel

from crawlee._types import JsonSerializable

from ._cookies import CookieParam
from ._session import Session


class SessionModel(BaseModel):
    """Model for a Session object."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, validate_by_name=True, validate_by_alias=True
    )

    id: Annotated[str, Field()]
    max_age: Annotated[timedelta, Field()]
    user_data: Annotated[MutableMapping[str, JsonSerializable], Field()]
    max_error_score: Annotated[float, Field()]
    error_score_decrement: Annotated[float, Field()]
    created_at: Annotated[datetime, Field()]
    usage_count: Annotated[int, Field()]
    max_usage_count: Annotated[int, Field()]
    error_score: Annotated[float, Field()]
    cookies: Annotated[list[CookieParam], Field()]
    blocked_status_codes: Annotated[list[int], Field()]


class SessionPoolModel(BaseModel):
    """Model for a SessionPool object."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, validate_by_name=True, validate_by_alias=True
    )

    max_pool_size: Annotated[int, Field()]

    sessions: Annotated[
        dict[
            str,
            Annotated[
                Session, GetPydanticSchema(lambda _, handler: handler(Any))
            ],  # handler(Any) is fine - we validate manually in the BeforeValidator
        ],
        Field(alias='sessions'),
        PlainSerializer(
            lambda value: [session.get_state().model_dump(by_alias=True) for session in value.values()],
            return_type=list,
        ),
        BeforeValidator(
            lambda value: {
                session.id: session
                for item in value
                if (session := Session.from_model(SessionModel.model_validate(item, by_alias=True)))
            }
        ),
    ]

    @computed_field(alias='sessionCount')
    @property
    def session_count(self) -> int:
        """Get the total number of sessions currently maintained in the pool."""
        return len(self.sessions)

    @computed_field(alias='usableSessionCount')
    @property
    def usable_session_count(self) -> int:
        """Get the number of sessions that are currently usable."""
        return len([session for _, session in self.sessions.items() if session.is_usable])

    @computed_field(alias='retiredSessionCount')
    @property
    def retired_session_count(self) -> int:
        """Get the number of sessions that are no longer usable."""
        return self.session_count - self.usable_session_count
