# Per-domain rate limit tracker for handling HTTP 429 responses.
# See: https://github.com/apify/crawlee-python/issues/1437

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging import getLogger
from urllib.parse import urlparse

from crawlee._utils.docs import docs_group

logger = getLogger(__name__)


@dataclass
class _DomainState:
    """Tracks rate limit state for a single domain."""

    domain: str
    """The domain being tracked."""

    next_allowed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Earliest time the next request to this domain is allowed."""

    consecutive_429_count: int = 0
    """Number of consecutive 429 responses (for exponential backoff)."""


@docs_group('Crawlers')
class RequestThrottler:
    """Per-domain rate limit tracker and request throttler.

    When a target website returns HTTP 429 (Too Many Requests), this component
    tracks the rate limit event per domain and applies exponential backoff.
    Requests to other (non-rate-limited) domains are unaffected.

    This solves the "death spiral" problem where 429 responses reduce CPU usage,
    causing the `AutoscaledPool` to incorrectly scale UP concurrency.
    """

    _BASE_DELAY = timedelta(seconds=2)
    """Initial delay after the first 429 response from a domain."""

    _MAX_DELAY = timedelta(seconds=60)
    """Maximum delay between requests to a rate-limited domain."""

    def __init__(self) -> None:
        self._domain_states: dict[str, _DomainState] = {}

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract the domain (hostname) from a URL.

        Args:
            url: The URL to extract the domain from.

        Returns:
            The hostname portion of the URL, or an empty string if parsing fails.
        """
        parsed = urlparse(url)
        return parsed.hostname or ''

    def record_rate_limit(self, url: str, *, retry_after: timedelta | None = None) -> None:
        """Record a 429 Too Many Requests response for the domain of the given URL.

        Increments the consecutive 429 count and calculates the next allowed
        request time using exponential backoff or the Retry-After value.

        Args:
            url: The URL that received a 429 response.
            retry_after: Optional delay from the Retry-After header. If provided,
                it takes priority over the calculated exponential backoff.
        """
        domain = self._extract_domain(url)
        if not domain:
            return

        now = datetime.now(timezone.utc)

        if domain not in self._domain_states:
            self._domain_states[domain] = _DomainState(domain=domain)

        state = self._domain_states[domain]
        state.consecutive_429_count += 1

        # Calculate delay: use Retry-After if provided, otherwise exponential backoff.
        if retry_after is not None:
            delay = retry_after
        else:
            delay = self._BASE_DELAY * (2 ** (state.consecutive_429_count - 1))

        # Cap the delay at _MAX_DELAY.
        if delay > self._MAX_DELAY:
            delay = self._MAX_DELAY

        state.next_allowed_at = now + delay

        logger.info(
            f'Rate limit (429) detected for domain "{domain}" '
            f'(consecutive: {state.consecutive_429_count}, delay: {delay.total_seconds():.1f}s)'
        )

    def is_throttled(self, url: str) -> bool:
        """Check if requests to the domain of the given URL should be delayed.

        Args:
            url: The URL to check.

        Returns:
            True if the domain is currently rate-limited and the cooldown has not expired.
        """
        domain = self._extract_domain(url)
        state = self._domain_states.get(domain)

        if state is None:
            return False

        return datetime.now(timezone.utc) < state.next_allowed_at

    def get_delay(self, url: str) -> timedelta:
        """Get the remaining delay before the next request to this domain is allowed.

        Args:
            url: The URL to check.

        Returns:
            The remaining time to wait. Returns zero if no delay is needed.
        """
        domain = self._extract_domain(url)
        state = self._domain_states.get(domain)

        if state is None:
            return timedelta(0)

        remaining = state.next_allowed_at - datetime.now(timezone.utc)
        return max(remaining, timedelta(0))

    def record_success(self, url: str) -> None:
        """Record a successful request to the domain, resetting its backoff state.

        Args:
            url: The URL that received a successful response.
        """
        domain = self._extract_domain(url)
        state = self._domain_states.get(domain)

        if state is not None and state.consecutive_429_count > 0:
            logger.debug(f'Resetting rate limit state for domain "{domain}" after successful request')
            state.consecutive_429_count = 0
