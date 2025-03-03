# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session.ts

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar, Literal, overload

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee.sessions._cookies import CookieParam, SessionCookies
from crawlee.sessions._models import SessionModel

if TYPE_CHECKING:
    from http.cookiejar import CookieJar

logger = getLogger(__name__)


@docs_group('Data structures')
class Session:
    """Represent a single user session, managing cookies, error states, and usage limits.

    A `Session` simulates a specific user with attributes like cookies, IP (via proxy), and potentially
    a unique browser fingerprint. It maintains its internal state, which can include custom user data
    (e.g., authorization tokens or headers) and tracks its usability through metrics such as error score,
    usage count, and expiration.
    """

    _DEFAULT_BLOCKED_STATUS_CODES: ClassVar = [401, 403, 429]
    """Default status codes that indicate a session is blocked."""

    def __init__(
        self,
        *,
        id: str | None = None,
        max_age: timedelta = timedelta(minutes=50),
        user_data: dict | None = None,
        max_error_score: float = 3.0,
        error_score_decrement: float = 0.5,
        created_at: datetime | None = None,
        usage_count: int = 0,
        max_usage_count: int = 50,
        error_score: float = 0.0,
        cookies: SessionCookies | CookieJar | dict[str, str] | list[CookieParam] | None = None,
        blocked_status_codes: list | None = None,
    ) -> None:
        """A default constructor.

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
            cookies: Cookies associated with the session.
            blocked_status_codes: HTTP status codes that indicate a session should be blocked.
        """
        self._id = id or crypto_random_object_id(length=10)
        self._max_age = max_age
        self._user_data = user_data or {}
        self._max_error_score = max_error_score
        self._error_score_decrement = error_score_decrement
        self._created_at = created_at or datetime.now(timezone.utc)
        self._usage_count = usage_count
        self._max_usage_count = max_usage_count
        self._error_score = error_score
        self._cookies = SessionCookies(cookies) or SessionCookies()
        self._blocked_status_codes = set(blocked_status_codes or self._DEFAULT_BLOCKED_STATUS_CODES)

    @classmethod
    def from_model(cls, model: SessionModel) -> Session:
        """Create a new instance from a `SessionModel`."""
        cookies = SessionCookies(model.cookies)
        return cls(**model.model_dump(exclude={'cookies'}), cookies=cookies)

    def __repr__(self) -> str:
        """Get a string representation."""
        return f'<{self.__class__.__name__} {self.get_state(as_dict=False)}>'

    def __eq__(self, other: object) -> bool:
        """Compare two sessions for equality."""
        if not isinstance(other, Session):
            return NotImplemented
        return self.get_state(as_dict=True) == other.get_state(as_dict=True)

    @property
    def id(self) -> str:
        """Get the session ID."""
        return self._id

    @property
    def user_data(self) -> dict:
        """Get the user data."""
        return self._user_data

    @property
    def cookies(self) -> SessionCookies:
        """Get the cookies."""
        return self._cookies

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
            cookies=self._cookies.get_cookies_as_dicts(),
            blocked_status_codes=self._blocked_status_codes,
        )
        if as_dict:
            return model.model_dump()
        return model

    def mark_good(self) -> None:
        """Mark the session as good. Should be called after a successful session usage."""
        self._usage_count += 1

        if self._error_score > 0:
            self._error_score = max(0, self._error_score - self._error_score_decrement)

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

    def is_blocked_status_code(
        self,
        *,
        status_code: int,
        ignore_http_error_status_codes: set[int] | None = None,
    ) -> bool:
        """Evaluate whether a session should be retired based on the received HTTP status code.

        Args:
            status_code: The HTTP status code received from a server response.
            ignore_http_error_status_codes: Optional status codes to allow suppression of
            codes from `blocked_status_codes`.

        Returns:
            True if the session should be retired, False otherwise.
        """
        return status_code in (self._blocked_status_codes - (ignore_http_error_status_codes or set()))
