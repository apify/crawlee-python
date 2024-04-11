# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session.ts

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.cookiejar import CookieJar
from logging import getLogger
from typing import ClassVar, Union

from crawlee._utils.crypto import crypto_random_object_id

logger = getLogger(__name__)

CookieTypes = Union[CookieJar, dict[str, str], list[tuple[str, str]]]


@dataclass
class UserData:
    pass


@dataclass
class SessionSettings:
    id: str = field(default_factory=lambda: crypto_random_object_id(10))
    max_age: timedelta = timedelta(seconds=3000)
    user_data: UserData = field(default_factory=UserData)
    max_error_score: float = 3
    error_score_decrement: float = 0.5
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    usage_count: int = 0
    max_usage_count: int = 50
    error_score: float = 0
    cookie_jar: CookieJar = field(default_factory=CookieJar)


class Session:
    """Session object represents a single user session.

    Sessions are used to store information such as cookies and can be used for generating fingerprints and proxy
    sessions. You can imagine each session as a specific user, with its own cookies, IP (via proxy) and potentially
    a unique browser fingerprint. Session internal state can be enriched with custom user data for example some
    authorization tokens and specific headers in general.
    """

    _DEFAULT_BLOCKED_STATUS_CODES: ClassVar = [401, 403, 429]

    def __init__(self, session_options: SessionSettings | None = None) -> None:
        session_options = session_options or SessionSettings()

        self.id = session_options.id
        self._max_age = session_options.max_age
        self._user_data = session_options.user_data
        self._max_error_score = session_options.max_error_score
        self._error_score_decrement = session_options.error_score_decrement
        self._created_at = session_options.created_at
        self._usage_count = session_options.usage_count
        self._max_usage_count = session_options.max_usage_count
        self._error_score = session_options.error_score
        self._cookie_jar = session_options.cookie_jar

        # reference to SessionPool ?

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

    @property
    def state(self) -> dict:
        """Returns the state of the session."""
        return {
            'id': self.id,
            'cookie_jar': self._cookie_jar,
            'user_data': self._user_data,
            'max_error_score': self._max_error_score,
            'error_score_decrement': self._error_score_decrement,
            'expires_at': self.expires_at.isoformat(),
            'created_at': self._created_at.isoformat(),
            'usage_count': self._usage_count,
            'max_usage_count': self._max_usage_count,
            'error_score': self._error_score,
        }

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

        # TODO: emit event so we can retire browser in puppeteer pool
        # this.sessionPool.emit(EVENT_SESSION_RETIRED, this);

    def _maybe_self_retire(self) -> None:
        """Retires the session if it's not usable anymore."""
        if not self.is_usable:
            self.retire()

    def mark_bad(self) -> None:
        """Marks the session as bad after an unsuccessful session usage."""
        self._error_score += 1
        self._usage_count += 1
        self._maybe_self_retire()

    def set_cookies(self, cookies: CookieTypes) -> None:
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

        error_messages: list[str] = []

        try:
            for cookie in cookies:
                self._cookie_jar.set_cookie(cookie)
        except Exception as e:
            error_messages.append(str(e))

        # If invalid Cookie header is provided just log the exception.
        if error_messages:
            logger.warning('Could not set cookies.', extra={'error_messages': error_messages})

    def get_cookies(self) -> CookieTypes:
        """Returns cookies in a format compatible with puppeteer/playwright."""
        return self._cookie_jar

    def retire_on_blocked_status_codes(
        self,
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
        blocked_status_codes = self._DEFAULT_BLOCKED_STATUS_CODES + (additional_blocked_status_codes or [])

        if status_code in blocked_status_codes:
            self.retire()
            return True

        return False

    # TODO: set cookies from response method?
    # TODO: get cookies as string method?
