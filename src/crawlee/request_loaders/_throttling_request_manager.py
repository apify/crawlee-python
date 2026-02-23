"""A request manager wrapper that enforces per-domain delays.

Handles both HTTP 429 backoff and robots.txt crawl-delay at the scheduling layer,
eliminating the busy-wait problem described in https://github.com/apify/crawlee-python/issues/1437.

Also addresses https://github.com/apify/crawlee-python/issues/1396 by providing a unified
delay mechanism for crawl-delay directives.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_manager import RequestManager

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request
    from crawlee.storage_clients.models import ProcessedRequest

logger = getLogger(__name__)


@dataclass
class _DomainState:
    """Tracks delay state for a single domain."""

    domain: str
    """The domain being tracked."""

    throttled_until: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Earliest time the next request to this domain is allowed."""

    consecutive_429_count: int = 0
    """Number of consecutive 429 responses (for exponential backoff)."""

    crawl_delay: timedelta | None = None
    """Minimum interval between requests from robots.txt crawl-delay directive."""

    last_request_at: datetime | None = None
    """When the last request to this domain was dispatched."""


@docs_group('Request loaders')
class ThrottlingRequestManager(RequestManager):
    """A request manager that wraps another and enforces per-domain delays.

    This moves throttling logic into the scheduling layer instead of the execution
    layer. When `fetch_next_request()` is called, it intelligently handles delays:

    - If the next request's domain is not throttled, it returns immediately.
    - If the domain is throttled but other requests are available, it buffers the
      throttled request and tries the next one.
    - If all available requests are throttled, it `asyncio.sleep()`s until the
      earliest domain cooldown expires — eliminating busy-wait and unnecessary
      queue writes.

    Delay sources:
    - HTTP 429 responses (via `record_domain_delay`)
    - robots.txt crawl-delay directives (via `set_crawl_delay`)
    """

    _BASE_DELAY = timedelta(seconds=2)
    """Initial delay after the first 429 response from a domain."""

    _MAX_DELAY = timedelta(seconds=60)
    """Maximum delay between requests to a rate-limited domain."""

    _MAX_BUFFER_SIZE = 50
    """Maximum number of requests to buffer before sleeping."""

    def __init__(self, inner: RequestManager) -> None:
        """Initialize the throttling manager.

        Args:
            inner: The underlying request manager to wrap (typically a RequestQueue).
        """
        self._inner = inner
        self._domain_states: dict[str, _DomainState] = {}
        self._buffered_requests: list[Request] = []

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract the domain (hostname) from a URL."""
        parsed = urlparse(url)
        return parsed.hostname or ''

    def _get_or_create_state(self, domain: str) -> _DomainState:
        """Get or create a domain state entry."""
        if domain not in self._domain_states:
            self._domain_states[domain] = _DomainState(domain=domain)
        return self._domain_states[domain]

    def _is_domain_throttled(self, domain: str) -> bool:
        """Check if a domain is currently throttled."""
        state = self._domain_states.get(domain)
        if state is None:
            return False

        now = datetime.now(timezone.utc)

        # Check 429 backoff.
        if now < state.throttled_until:
            return True

        # Check crawl-delay: enforce minimum interval between requests.
        if state.crawl_delay is not None and state.last_request_at is not None:
            if now < state.last_request_at + state.crawl_delay:
                return True

        return False

    def _get_earliest_available_time(self) -> datetime:
        """Get the earliest time any throttled domain becomes available."""
        now = datetime.now(timezone.utc)
        earliest = now + self._MAX_DELAY  # Fallback upper bound.

        for state in self._domain_states.values():
            # Consider 429 backoff.
            if state.throttled_until > now and state.throttled_until < earliest:
                earliest = state.throttled_until

            # Consider crawl-delay.
            if state.crawl_delay is not None and state.last_request_at is not None:
                next_allowed = state.last_request_at + state.crawl_delay
                if next_allowed > now and next_allowed < earliest:
                    earliest = next_allowed

        return earliest

    def record_domain_delay(self, url: str, *, retry_after: timedelta | None = None) -> None:
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
        state = self._get_or_create_state(domain)
        state.consecutive_429_count += 1

        # Calculate delay: use Retry-After if provided, otherwise exponential backoff.
        if retry_after is not None:
            delay = retry_after
        else:
            delay = self._BASE_DELAY * (2 ** (state.consecutive_429_count - 1))

        # Cap the delay.
        if delay > self._MAX_DELAY:
            delay = self._MAX_DELAY

        state.throttled_until = now + delay

        logger.info(
            f'Rate limit (429) detected for domain "{domain}" '
            f'(consecutive: {state.consecutive_429_count}, delay: {delay.total_seconds():.1f}s)'
        )

    def record_success(self, url: str) -> None:
        """Record a successful request, resetting the backoff state for that domain.

        Args:
            url: The URL that received a successful response.
        """
        domain = self._extract_domain(url)
        state = self._domain_states.get(domain)

        if state is not None and state.consecutive_429_count > 0:
            logger.debug(f'Resetting rate limit state for domain "{domain}" after successful request')
            state.consecutive_429_count = 0

    def set_crawl_delay(self, url: str, delay_seconds: int) -> None:
        """Set the robots.txt crawl-delay for a domain.

        Args:
            url: A URL from the domain to throttle.
            delay_seconds: The crawl-delay value in seconds.
        """
        domain = self._extract_domain(url)
        if not domain:
            return

        state = self._get_or_create_state(domain)
        state.crawl_delay = timedelta(seconds=delay_seconds)

        logger.debug(f'Set crawl-delay for domain "{domain}" to {delay_seconds}s')

    def _mark_domain_dispatched(self, url: str) -> None:
        """Record that a request to this domain was just dispatched."""
        domain = self._extract_domain(url)
        if domain:
            state = self._get_or_create_state(domain)
            state.last_request_at = datetime.now(timezone.utc)

    # ──────────────────────────────────────────────────────
    # RequestManager interface delegation + smart scheduling
    # ──────────────────────────────────────────────────────

    @override
    async def drop(self) -> None:
        self._buffered_requests.clear()
        await self._inner.drop()

    @override
    async def add_request(self, request: str | Request, *, forefront: bool = False) -> ProcessedRequest:
        return await self._inner.add_request(request, forefront=forefront)

    @override
    async def add_requests(
        self,
        requests: Sequence[str | Request],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: timedelta | None = None,
    ) -> None:
        return await self._inner.add_requests(
            requests,
            forefront=forefront,
            batch_size=batch_size,
            wait_time_between_batches=wait_time_between_batches,
            wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
            wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
        )

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        return await self._inner.reclaim_request(request, forefront=forefront)

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        return await self._inner.mark_request_as_handled(request)

    @override
    async def get_handled_count(self) -> int:
        return await self._inner.get_handled_count()

    @override
    async def get_total_count(self) -> int:
        return await self._inner.get_total_count()

    @override
    async def is_empty(self) -> bool:
        if self._buffered_requests:
            return False
        return await self._inner.is_empty()

    @override
    async def is_finished(self) -> bool:
        if self._buffered_requests:
            return False
        return await self._inner.is_finished()

    @override
    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request, respecting per-domain delays.

        If the next available request belongs to a throttled domain, buffer it and
        try the next one. If all available requests are throttled, sleep until the
        earliest domain becomes available.
        """
        # First, check if any buffered requests are now unthrottled.
        still_throttled = []
        for req in self._buffered_requests:
            domain = self._extract_domain(req.url)
            if not self._is_domain_throttled(domain):
                self._mark_domain_dispatched(req.url)
                # Return remaining throttled requests to buffer.
                self._buffered_requests = still_throttled
                return req
            still_throttled.append(req)
        self._buffered_requests = still_throttled

        # Try fetching from the inner queue.
        while True:
            request = await self._inner.fetch_next_request()

            if request is None:
                # No more requests in the queue.
                if self._buffered_requests:
                    # There are buffered requests waiting for cooldown — sleep and retry.
                    earliest = self._get_earliest_available_time()
                    sleep_duration = max(
                        (earliest - datetime.now(timezone.utc)).total_seconds(),
                        0.1,  # Minimum sleep to avoid tight loops.
                    )
                    logger.debug(
                        f'All {len(self._buffered_requests)} buffered request(s) throttled. '
                        f'Sleeping {sleep_duration:.1f}s until earliest domain is available.'
                    )
                    await asyncio.sleep(sleep_duration)
                    # After sleep, recursively try again.
                    return await self.fetch_next_request()
                return None

            domain = self._extract_domain(request.url)

            if not self._is_domain_throttled(domain):
                # Domain is clear — dispatch immediately.
                self._mark_domain_dispatched(request.url)
                return request

            # Domain is throttled — buffer this request.
            logger.debug(
                f'Request to {request.url} buffered — domain "{domain}" is throttled'
            )
            self._buffered_requests.append(request)

            if len(self._buffered_requests) >= self._MAX_BUFFER_SIZE:
                # Too many buffered: sleep until earliest cooldown and retry.
                earliest = self._get_earliest_available_time()
                sleep_duration = max(
                    (earliest - datetime.now(timezone.utc)).total_seconds(),
                    0.1,
                )
                logger.debug(
                    f'Buffer full ({self._MAX_BUFFER_SIZE} requests). '
                    f'Sleeping {sleep_duration:.1f}s.'
                )
                await asyncio.sleep(sleep_duration)
                return await self.fetch_next_request()
