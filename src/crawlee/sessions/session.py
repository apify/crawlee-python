# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session.ts

# TODO:
# - Implement UserData
# - Implement Cookies
#   - set cookies from response method?
#   - get cookies as string method?
#   - normalization? url?

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import ClassVar, Literal, overload

from crawlee._utils.crypto import crypto_random_object_id
from crawlee.sessions.models import CookieJar, SessionModel, UserData

logger = getLogger(__name__)


class Session:
    """Session object represents a single user session.

    Sessions are used to store information such as cookies and can be used for generating fingerprints and proxy
    sessions. You can imagine each session as a specific user, with its own cookies, IP (via proxy) and potentially
    a unique browser fingerprint. Session internal state can be enriched with custom user data for example some
    authorization tokens and specific headers in general.
    """

    _DEFAULT_BLOCKED_STATUS_CODES: ClassVar = [401, 403, 429]

    def __init__(
        self,
        id: str | None = None,  # noqa: A002
        max_age: timedelta = timedelta(minutes=50),
        user_data: UserData | None = None,
        max_error_score: float = 3.0,
        error_score_decrement: float = 0.5,
        created_at: datetime | None = None,
        usage_count: int = 0,
        max_usage_count: int = 50,
        error_score: float = 0.0,
        cookie_jar: CookieJar | None = None,
        blocked_status_codes: list | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            id: Unique identifier for the session, autogenerated if not provided.
            max_age: Time duration after which the session expires.
            user_data: Custom user data associated with the session.
            max_error_score: Threshold score beyond which the session is considered blocked.
            error_score_decrement: Value by which the error score is decremented on successful operations.
            created_at: Timestamp when the session was created, defaults to current UTC time if not provided.
            usage_count: Number of times the session has been used.
            max_usage_count: Maximum allowable uses of the session before it is considered expired.
            error_score: Current error score of the session.
            cookie_jar: Container for cookies associated with the session.
            blocked_status_codes: HTTP status codes that indicate a session should be blocked.
        """
        self._id = id or crypto_random_object_id(length=10)
        self._max_age = max_age
        self._user_data = user_data or UserData()
        self._max_error_score = max_error_score
        self._error_score_decrement = error_score_decrement
        self._created_at = created_at or datetime.now(timezone.utc)
        self._usage_count = usage_count
        self._max_usage_count = max_usage_count
        self._error_score = error_score
        self._cookie_jar = cookie_jar or CookieJar()
        self._blocked_status_codes = blocked_status_codes or self._DEFAULT_BLOCKED_STATUS_CODES

    @classmethod
    def from_model(cls, model: SessionModel) -> Session:
        """Create a new instance from a SessionModel."""
        return cls(**model.model_dump())

    def __repr__(self) -> str:
        """Get a string representation."""
        return f'<{self.__class__.__name__} {self.get_state(as_dict=False)}>'

    @property
    def id(self) -> str:
        """Get the session ID."""
        return self._id

    @property
    def error_score(self) -> float:
        """Get the current error score."""
        return self._error_score

    @property
    def usage_count(self) -> float:
        """Get the current usage count."""
        return self._usage_count

    @property
    def expires_at(self) -> datetime:
        """Get the expiration datetime of the session."""
        return self._created_at + self._max_age

    @property
    def is_blocked(self) -> bool:
        """Indicate whether the session is blocked based on the error score.."""
        return self._error_score >= self._max_error_score

    @property
    def is_expired(self) -> bool:
        """Indicate whether the session is expired based on the current time."""
        return datetime.now(timezone.utc) >= self.expires_at

    @property
    def is_max_usage_count_reached(self) -> bool:
        """Indicate whether the session has reached its maximum usage limit."""
        return self._usage_count >= self._max_usage_count

    @property
    def is_usable(self) -> bool:
        """Determine if the session is usable for next requests."""
        return not (self.is_blocked or self.is_expired or self.is_max_usage_count_reached)

    @overload
    def get_state(self, *, as_dict: Literal[True]) -> dict: ...

    @overload
    def get_state(self, *, as_dict: Literal[False]) -> SessionModel: ...

    def get_state(self, *, as_dict: bool = False) -> SessionModel | dict:
        """Retrieve the current state of the session either as a model or as a dictionary."""
        model = SessionModel(
            id=self._id,
            max_age=self._max_age,
            user_data=self._user_data,
            max_error_score=self._max_error_score,
            error_score_decrement=self._error_score_decrement,
            created_at=self._created_at,
            usage_count=self._usage_count,
            max_usage_count=self._max_usage_count,
            error_score=self._error_score,
            cookie_jar=self._cookie_jar,
            blocked_status_codes=self._blocked_status_codes,
        )
        if as_dict:
            return model.model_dump()
        return model

    def mark_good(self) -> None:
        """Mark the session as good. Should be called after a successful session usage."""
        self._usage_count += 1

        if self._error_score > 0:
            self._error_score -= self._error_score_decrement

        # Retire the session if it is not usable anymore
        if not self.is_usable:
            self.retire()

    def mark_bad(self) -> None:
        """Mark the session as bad after an unsuccessful session usage."""
        self._error_score += 1
        self._usage_count += 1

        # Retire the session if it is not usable anymore
        if not self.is_usable:
            self.retire()

    def retire(self) -> None:
        """Retire the session by setting the error score to the maximum value.

        This method should be used if the session usage was unsuccessful and you are sure that it is because of
        the session configuration and not any external matters. For example when server returns 403 status code.
        If the session does not work due to some external factors as server error such as 5XX you probably want
        to use `mark_bad` method.
        """
        self._error_score += self._max_error_score
        self._usage_count += 1
        # Note: We emit an event here because of the Puppeteer in TS implementation.

    def retire_on_blocked_status_codes(
        self,
        *,
        status_code: int,
        additional_blocked_status_codes: list[int] | None = None,
    ) -> bool:
        """Evaluate whether a session should be retired based on the received HTTP status code.

        Args:
            status_code: The HTTP status code received from a server response.
            additional_blocked_status_codes: Optional additional status codes that should trigger session retirement.

        Returns:
            True if the session was retired, False otherwise.
        """
        blocked_status_codes = self._blocked_status_codes + (additional_blocked_status_codes or [])

        if status_code in blocked_status_codes:
            self.retire()
            return True

        return False
