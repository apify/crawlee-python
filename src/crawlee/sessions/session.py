# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session.ts

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import Any, ClassVar

from dateutil import parser
from pydantic import BaseModel, field_validator

from crawlee._utils.crypto import crypto_random_object_id

logger = getLogger(__name__)


@dataclass
class UserData:
    # TODO: implement user data
    pass


class _SessionModel(BaseModel):
    id: str
    max_age: timedelta
    user_data: UserData
    max_error_score: float
    error_score_decrement: float
    created_at: datetime
    usage_count: int
    max_usage_count: int
    error_score: float
    cookie_jar: dict
    blocked_status_codes: list[int]

    @field_validator('max_age', mode='before')
    def parse_max_age(cls, v: Any) -> timedelta:  # noqa: N805
        if isinstance(v, timedelta):
            return v

        if isinstance(v, str):
            try:
                parsed_time = parser.parse(v)
                return timedelta(hours=parsed_time.hour, minutes=parsed_time.minute, seconds=parsed_time.second)
            except ValueError as exc:
                raise ValueError(f"Invalid time format for max_age. Expected 'HH:MM:SS', got {v}") from exc

        raise ValueError('Invalid data type for max_age')

    @field_validator('created_at', mode='before')
    def parse_created_at(cls, v: Any) -> datetime:  # noqa: N805
        if isinstance(v, str):
            return datetime.fromisoformat(v)

        if isinstance(v, datetime):
            return v

        raise ValueError('Invalid data type for created_at')


class Session:
    """Session object represents a single user session.

    Sessions are used to store information such as cookies and can be used for generating fingerprints and proxy
    sessions. You can imagine each session as a specific user, with its own cookies, IP (via proxy) and potentially
    a unique browser fingerprint. Session internal state can be enriched with custom user data for example some
    authorization tokens and specific headers in general.

    TODO: Add args description
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
        cookie_jar: dict | None = None,
        blocked_status_codes: list | None = None,
    ) -> None:
        self._id = id or crypto_random_object_id(length=10)
        self._max_age = max_age
        self._user_data = user_data or UserData()
        self._max_error_score = max_error_score
        self._error_score_decrement = error_score_decrement
        self._created_at = created_at or datetime.now(timezone.utc)
        self._usage_count = usage_count
        self._max_usage_count = max_usage_count
        self._error_score = error_score
        self._cookie_jar = cookie_jar or {}
        self._blocked_status_codes = blocked_status_codes or self._DEFAULT_BLOCKED_STATUS_CODES

    @classmethod
    def from_kwargs(cls, **kwargs: Any) -> Session:
        """Creates a session from a dictionary."""
        model = _SessionModel(**kwargs)
        return cls(**model.model_dump())

    def __repr__(self) -> str:
        """Returns a string representation of the session."""
        return f'<{self.__class__.__name__} {self.get_state()}>'

    @property
    def id(self) -> str:
        """Returns the session ID."""
        return self._id

    @property
    def expires_at(self) -> datetime:
        """Returns the datetime when the session expires."""
        return self._created_at + self._max_age

    @property
    def is_blocked(self) -> bool:
        """Indicates whether the session is blocked."""
        return self._error_score >= self._max_error_score

    @property
    def is_expired(self) -> bool:
        """Indicates whether the session is expired."""
        return self.expires_at <= datetime.now(timezone.utc)

    @property
    def is_max_usage_count_reached(self) -> bool:
        """Indicates whether the session is used maximum number of times."""
        return self._usage_count >= self._max_usage_count

    @property
    def is_usable(self) -> bool:
        """Indicates whether the session can be used for next requests."""
        return not (self.is_blocked and self.is_expired and self.is_max_usage_count_reached)

    def get_state(self) -> dict:
        """Returns the session state for persistence."""
        return _SessionModel(
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
        ).model_dump()

    def mark_good(self) -> None:
        """Marks the session as good after a successful session usage."""
        self._usage_count += 1
        if self._error_score > 0:
            self._error_score -= self._error_score_decrement
        self._maybe_self_retire()

    def retire(self) -> None:
        """Marks the session as blocked, increments usage, and emits a retirement event.

        Marks session as blocked and emits event on the `SessionPool`. This method should be used if the session usage
        was unsuccessful and you are sure that it is because of the session configuration and not any external matters.
        For example when server returns 403 status code. If the session does not work due to some external factors
        as server error such as 5XX you probably want to use `mark_bad` method.
        """
        self._error_score += self._max_error_score
        self._usage_count += 1

    def mark_bad(self) -> None:
        """Marks the session as bad after an unsuccessful session usage."""
        self._error_score += 1
        self._usage_count += 1
        self._maybe_self_retire()

    def _maybe_self_retire(self) -> None:
        """Retires the session if it's not usable anymore."""
        if not self.is_usable:
            self.retire()

    def set_cookies(self, cookies: dict) -> None:
        """Saves an array with cookie objects to be used with the session.

            The objects should be in the format that
            [Puppeteer uses](https://pptr.dev/#?product=Puppeteer&version=v2.0.0&show=api-pagecookiesurls),
            but you can also use this function to set cookies manually:

        Args:
            cookies (CookieTypes): _description_
        """
        # TODO: normalization? url?
        # setCookies(cookies: CookieObject[], url: string) {
        # const normalizedCookies = cookies.map((c) => browserPoolCookieToToughCookie(c, this.maxAgeSecs));
        # this._setCookies(normalizedCookies, url);
        # it seems cookies are set like a: url: cookies

        # error_messages: list[str] = []
        # try:
        #     for cookie in cookies:
        #         self._cookie_jar.set_cookie(cookie)
        # except Exception as e:
        #     error_messages.append(str(e))
        # if error_messages:
        #     logger.warning('Could not set cookies.', extra={'error_messages': error_messages})

    # def get_cookies(self) -> dict:
    #     """Returns cookies in a format compatible with puppeteer/playwright."""
    #     return self.cookie_jar

    def retire_on_blocked_status_codes(
        self,
        *,
        status_code: int,
        additional_blocked_status_codes: list[int] | None = None,
    ) -> bool:
        """Retires the session when certain status codes are received.

        Args:
            status_code: HTTP status code.
            additional_blocked_status_codes: Custom HTTP status codes that means blocking on particular website.

        Returns:
            True if the session was retired, False otherwise.
        """
        blocked_status_codes = self._blocked_status_codes + (additional_blocked_status_codes or [])

        if status_code in blocked_status_codes:
            self.retire()
            return True

        return False

    # TODO: set cookies from response method?
    # TODO: get cookies as string method?
