from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetPydanticSchema,
    PlainSerializer,
    PlainValidator,
    TypeAdapter,
    computed_field,
)

from ._cookies import CookieParam
from ._session import Session


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


session_list_adapter = TypeAdapter(list[SessionModel])


def load_sessions(value: Any) -> Any:
    try:
        list_result = session_list_adapter.validate_python(value)
        return {session.id: Session.from_model(session) for session in list_result}
    except ValueError:
        return value


class SessionPoolModel(BaseModel):
    """Model for a SessionPool object."""

    model_config = ConfigDict(populate_by_name=True)

    max_pool_size: Annotated[int, Field(alias='maxPoolSize')]

    sessions: Annotated[
        dict[
            str,
            Annotated[
                Session,
                GetPydanticSchema(lambda _, handler: handler(SessionModel)),
            ],
        ],
        Field(alias='sessions'),
        PlainSerializer(
            lambda value: session_list_adapter.dump_python(
                [session.get_state(as_dict=False) for session in value.values()]
            ),
            return_type=list[SessionModel],
        ),
        PlainValidator(load_sessions),
    ]

    @computed_field(alias='sessionCount')  # type: ignore[prop-decorator]
    @property
    def session_count(self) -> int:
        """Get the total number of sessions currently maintained in the pool."""
        return len(self.sessions)

    @computed_field(alias='usableSessionCount')  # type: ignore[prop-decorator]
    @property
    def usable_session_count(self) -> int:
        """Get the number of sessions that are currently usable."""
        return len([session for _, session in self.sessions.items() if session.is_usable])

    @computed_field(alias='retiredSessionCount')  # type: ignore[prop-decorator]
    @property
    def retired_session_count(self) -> int:
        """Get the number of sessions that are no longer usable."""
        return self.session_count - self.usable_session_count
