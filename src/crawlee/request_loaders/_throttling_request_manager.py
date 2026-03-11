"""A request manager wrapper that enforces per-domain delays.

Handles both HTTP 429 backoff and robots.txt crawl-delay at the scheduling layer,
routing requests for explicitly configured domains into dedicated sub-queues and
applying intelligent delay-aware scheduling.
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
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request
    from crawlee.storage_clients.models import ProcessedRequest

from crawlee._service_locator import ServiceLocator
from crawlee._service_locator import service_locator as global_service_locator

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
    """Minimum interval between requests, used to push `throttled_until` on dispatch."""


@docs_group('Request loaders')
class ThrottlingRequestManager(RequestManager):
    """A request manager that wraps another and enforces per-domain delays.

    Requests for explicitly configured domains are routed into dedicated sub-queues
    at insertion time — each request lives in exactly one queue, eliminating
    duplication and simplifying deduplication.

    When `fetch_next_request()` is called, it returns requests from the sub-queue
    whose domain has been waiting the longest. If all configured domains are
    throttled, it falls back to the inner queue for non-throttled domains. If the
    inner queue is also empty and all sub-queues are throttled, it sleeps until the
    earliest cooldown expires.

    Delay sources:
    - HTTP 429 responses (via `record_domain_delay`)
    - robots.txt crawl-delay directives (via `set_crawl_delay`)

    Example:
        ```python
        from crawlee.storages import RequestQueue
        from crawlee.request_loaders import ThrottlingRequestManager

        queue = await RequestQueue.open()
        throttler = ThrottlingRequestManager(
            queue,
            domains=['api.example.com', 'slow-site.org'],
        )
        crawler = BasicCrawler(request_manager=throttler)
        ```
    """

    def __init__(
        self,
        inner: RequestManager,
        *,
        domains: Sequence[str],
        service_locator: ServiceLocator | None = None,
        base_delay: timedelta = timedelta(seconds=2),
        max_delay: timedelta = timedelta(seconds=60),
    ) -> None:
        """Initialize the throttling manager.

        Args:
            inner: The underlying request manager to wrap (typically a RequestQueue).
                Requests for non-throttled domains are stored here.
            domains: Explicit list of domain hostnames to throttle. Only requests
                matching these domains will be routed to per-domain sub-queues.
            service_locator: Service locator for creating sub-queues. If not
                provided, defaults to the global service locator, ensuring
                consistency with the crawler's storage backend.
            base_delay: Initial delay after the first 429 response from a domain.
            max_delay: Maximum delay between requests to a rate-limited domain.
        """
        self._inner = inner
        self._service_locator = service_locator if service_locator is not None else global_service_locator
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._domain_states: dict[str, _DomainState] = {d: _DomainState(domain=d) for d in domains}
        self._sub_queues: dict[str, RequestQueue] = {}

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract the domain (hostname) from a URL."""
        parsed = urlparse(url)
        return parsed.hostname or ''

    def _get_url_from_request(self, request: str | Request) -> str:
        """Extract URL string from a request that may be a string or Request object."""
        if isinstance(request, str):
            return request
        return request.url

    async def _get_or_create_sub_queue(self, domain: str) -> RequestQueue:
        """Get or create a per-domain sub-queue."""
        if domain not in self._sub_queues:
            self._sub_queues[domain] = await RequestQueue.open(
                alias=f'throttled-{domain}',
                storage_client=self._service_locator.get_storage_client(),
                configuration=self._service_locator.get_configuration(),
            )
        return self._sub_queues[domain]

    def _is_domain_throttled(self, domain: str) -> bool:
        """Check if a domain is currently throttled."""
        state = self._domain_states.get(domain)
        if state is None:
            return False
        return datetime.now(timezone.utc) < state.throttled_until

    def _get_earliest_available_time(self) -> datetime:
        """Get the earliest time any throttled domain becomes available."""
        now = datetime.now(timezone.utc)
        earliest = now + self._max_delay  # Fallback upper bound.

        for state in self._domain_states.values():
            if state.throttled_until > now and state.throttled_until < earliest:
                earliest = state.throttled_until

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

        state = self._domain_states.get(domain)
        if state is None:
            return

        now = datetime.now(timezone.utc)
        state.consecutive_429_count += 1

        # Calculate delay: use Retry-After if provided, otherwise exponential backoff.
        delay = retry_after if retry_after is not None else self._base_delay * (2 ** (state.consecutive_429_count - 1))

        # Cap the delay.
        delay = min(delay, self._max_delay)

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
        if not domain:
            return

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

        state = self._domain_states.get(domain)
        if state is None:
            return

        state.crawl_delay = timedelta(seconds=delay_seconds)

        logger.debug(f'Set crawl-delay for domain "{domain}" to {delay_seconds}s')

    def _mark_domain_dispatched(self, url: str) -> None:
        """Record that a request to this domain was just dispatched.

        If a crawl-delay is configured, push throttled_until forward by that amount.
        """
        domain = self._extract_domain(url)
        if not domain:
            return

        state = self._domain_states.get(domain)
        if state is None:
            return

        # If crawl-delay is set, enforce minimum interval by pushing throttled_until.
        if state.crawl_delay is not None:
            state.throttled_until = datetime.now(timezone.utc) + state.crawl_delay

    async def recreate_purged(self) -> ThrottlingRequestManager:
        """Drop all queues and return a fresh ThrottlingRequestManager with the same configuration.

        This is used during crawler purge to reconstruct the throttler with empty
        queues while preserving domain configuration and service locator.

        Note: The inner manager is always recreated as a ``RequestQueue``.
        """
        await self.drop()

        inner = await RequestQueue.open(
            storage_client=self._service_locator.get_storage_client(),
            configuration=self._service_locator.get_configuration(),
        )

        return ThrottlingRequestManager(
            inner,
            domains=list(self._domain_states.keys()),
            service_locator=self._service_locator,
            base_delay=self._base_delay,
            max_delay=self._max_delay,
        )

    @override
    async def drop(self) -> None:
        await self._inner.drop()
        for sq in self._sub_queues.values():
            await sq.drop()
        self._sub_queues.clear()

    @override
    async def add_request(self, request: str | Request, *, forefront: bool = False) -> ProcessedRequest:
        """Add a request, routing it to the appropriate queue.

        Requests for explicitly configured domains are routed directly to their
        per-domain sub-queue. All other requests go to the inner queue.
        """
        url = self._get_url_from_request(request)
        domain = self._extract_domain(url)

        if domain in self._domain_states:
            sq = await self._get_or_create_sub_queue(domain)
            return await sq.add_request(request, forefront=forefront)

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
        """Add multiple requests, routing each to the appropriate queue."""
        inner_requests: list[str | Request] = []
        domain_requests: dict[str, list[str | Request]] = {}

        for request in requests:
            url = self._get_url_from_request(request)
            domain = self._extract_domain(url)

            if domain in self._domain_states:
                domain_requests.setdefault(domain, []).append(request)
            else:
                inner_requests.append(request)

        # Add non-throttled requests to inner queue.
        if inner_requests:
            await self._inner.add_requests(
                inner_requests,
                forefront=forefront,
                batch_size=batch_size,
                wait_time_between_batches=wait_time_between_batches,
                wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
                wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
            )

        # Add throttled requests to their respective sub-queues.
        for domain, reqs in domain_requests.items():
            sq = await self._get_or_create_sub_queue(domain)
            await sq.add_requests(
                reqs,
                forefront=forefront,
                batch_size=batch_size,
                wait_time_between_batches=wait_time_between_batches,
                wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
                wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
            )

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        domain = self._extract_domain(request.url)
        if domain in self._domain_states and domain in self._sub_queues:
            return await self._sub_queues[domain].reclaim_request(request, forefront=forefront)
        return await self._inner.reclaim_request(request, forefront=forefront)

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        domain = self._extract_domain(request.url)
        if domain in self._domain_states and domain in self._sub_queues:
            return await self._sub_queues[domain].mark_request_as_handled(request)
        return await self._inner.mark_request_as_handled(request)

    @override
    async def get_handled_count(self) -> int:
        count = await self._inner.get_handled_count()
        for sq in self._sub_queues.values():
            count += await sq.get_handled_count()
        return count

    @override
    async def get_total_count(self) -> int:
        count = await self._inner.get_total_count()
        for sq in self._sub_queues.values():
            count += await sq.get_total_count()
        return count

    @override
    async def is_empty(self) -> bool:
        if not await self._inner.is_empty():
            return False
        for sq in self._sub_queues.values():
            if not await sq.is_empty():
                return False
        return True

    @override
    async def is_finished(self) -> bool:
        if not await self._inner.is_finished():
            return False
        for sq in self._sub_queues.values():
            if not await sq.is_finished():
                return False
        return True

    @override
    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request, respecting per-domain delays.

        Sub-queues are checked in order of longest-overdue domain first
        (sorted by `throttled_until` ascending). If all configured domains are
        throttled, falls back to the inner queue for non-throttled domains.
        If the inner queue is also empty and all sub-queues are throttled,
        sleeps until the earliest domain becomes available.
        """
        while True:
            # Collect unthrottled domains and sort by throttled_until (longest-overdue first).
            available_domains = [
                domain
                for domain in self._domain_states
                if domain in self._sub_queues and not self._is_domain_throttled(domain)
            ]
            available_domains.sort(
                key=lambda d: self._domain_states[d].throttled_until,
            )

            for domain in available_domains:
                sq = self._sub_queues[domain]
                req = await sq.fetch_next_request()
                if req:
                    self._mark_domain_dispatched(req.url)
                    return req

            # Try fetching from the inner queue (non-throttled domains).
            request = await self._inner.fetch_next_request()
            if request is not None:
                return request

            # No requests in inner queue. Check if any sub-queues still have requests.
            have_sq_requests = False
            for sq in self._sub_queues.values():
                if not await sq.is_empty():
                    have_sq_requests = True
                    break

            if not have_sq_requests:
                return None

            # Requests exist but all domains are throttled and inner is empty. Sleep and retry.
            earliest = self._get_earliest_available_time()
            sleep_duration = max(
                (earliest - datetime.now(timezone.utc)).total_seconds(),
                0.1,  # Minimum sleep to avoid tight loops.
            )
            logger.debug(
                f'All configured domains are throttled and inner queue is empty. '
                f'Sleeping {sleep_duration:.1f}s until earliest domain is available.'
            )
            await asyncio.sleep(sleep_duration)
