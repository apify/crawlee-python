from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

from typing_extensions import override
from yarl import URL

from crawlee._service_locator import ServiceLocator
from crawlee._service_locator import service_locator as global_service_locator
from crawlee._utils.docs import docs_group
from crawlee.request_loaders._request_manager import RequestManager

if TYPE_CHECKING:
    from collections.abc import Sequence

    from crawlee._request import Request
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient
    from crawlee.storage_clients.models import ProcessedRequest

logger = getLogger(__name__)

TRequestManager = TypeVar('TRequestManager', bound=RequestManager)

_NEVER_THROTTLED = datetime.min.replace(tzinfo=timezone.utc)
"""Sentinel `throttled_until` value meaning the domain has no active backoff."""


@docs_group('Request loaders')
class ThrottlingRequestManager(RequestManager, Generic[TRequestManager]):
    """A request manager that wraps another and enforces per-domain delays.

    Requests for explicitly configured domains are routed into dedicated sub-managers at insertion time — each request
    lives in exactly one manager, eliminating duplication and simplifying deduplication.

    When `fetch_next_request()` is called, it returns requests from the sub-manager whose domain has been waiting the
    longest. If all configured domains are throttled, it falls back to the inner manager for non-throttled domains. If
    the inner manager is also empty and all sub-managers are throttled, it sleeps until the earliest cooldown expires.

    Delay sources:
    - HTTP 429 responses (via `record_domain_delay`)
    - robots.txt crawl-delay directives (via `set_crawl_delay`)

    The class is generic over the wrapped manager type. The `request_manager_opener` callback is used to construct
    per-domain sub-managers at insertion time, so every sub-manager shares the same `RequestManager` subclass and
    backing store as `inner`. The opener must accept `alias`, `storage_client`, and `configuration` keyword arguments
    (as `RequestQueue.open` does) and return the same concrete subclass as `inner`.

    ### Usage

    ```python
    from crawlee.crawlers import BasicCrawler
    from crawlee.request_loaders import ThrottlingRequestManager
    from crawlee.storages import RequestQueue

    queue = await RequestQueue.open()
    throttler = ThrottlingRequestManager(
        inner=queue,
        domains=['api.example.com', 'slow-site.org'],
        request_manager_opener=RequestQueue.open,
    )
    crawler = BasicCrawler(request_manager=throttler)
    ```
    """

    def __init__(
        self,
        inner: TRequestManager,
        *,
        domains: Sequence[str],
        request_manager_opener: _RequestManagerOpener[TRequestManager],
        service_locator: ServiceLocator | None = None,
        base_delay: timedelta = timedelta(seconds=2),
        max_delay: timedelta = timedelta(seconds=60),
    ) -> None:
        """Initialize the throttling manager.

        Args:
            inner: The underlying request manager to wrap (typically a `RequestQueue`). Requests for non-throttled
                domains are stored here.
            domains: Explicit list of domain hostnames to throttle. Only requests matching these domains will be routed
                to per-domain sub-managers. Matching is case-insensitive (hostnames are lowercased) and exact: subdomain
                wildcards such as `*.example.com` are not supported — list each subdomain explicitly if needed.
            request_manager_opener: Async callable used to create per-domain sub-managers at insertion time. Must
                accept `alias`, `storage_client`, and `configuration` keyword arguments and return the same concrete
                subclass as `inner` (e.g. `RequestQueue.open` when `inner` is a `RequestQueue`).
            service_locator: Service locator for creating sub-managers. If not provided, defaults to the global service
                locator, ensuring consistency with the crawler's storage backend.
            base_delay: Initial delay after the first 429 response from a domain.
            max_delay: Maximum delay between requests to a rate-limited domain.
        """
        self._inner: TRequestManager = inner
        self._service_locator = service_locator if service_locator is not None else global_service_locator
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._request_manager_opener = request_manager_opener
        self._domain_states: dict[str, _DomainState] = {d.lower(): _DomainState(domain=d.lower()) for d in domains if d}
        self._sub_managers: dict[str, TRequestManager] = {}
        self._new_work_event = asyncio.Event()
        """Set whenever a request is added or reclaimed. Lets `fetch_next_request` wake from a throttle
        wait early when fresh work appears, instead of sleeping for the full computed cooldown."""

    @override
    async def drop(self) -> None:
        await asyncio.gather(self._inner.drop(), *(sm.drop() for sm in self._sub_managers.values()))
        self._sub_managers.clear()

    @override
    async def purge(self) -> None:
        """Empty the inner manager and all sub-managers, and reset transient per-domain throttle state.

        The configured domain list and any robots.txt-derived `crawl_delay` are preserved; only the dynamic backoff
        state (consecutive 429 counter and `throttled_until`) is cleared. Sub-managers are kept around so they don't
        need to be re-opened on the next request — they're just emptied.
        """
        await asyncio.gather(self._inner.purge(), *(sm.purge() for sm in self._sub_managers.values()))
        for state in self._domain_states.values():
            state.consecutive_429_count = 0
            state.throttled_until = _NEVER_THROTTLED

    @override
    async def add_request(self, request: str | Request, *, forefront: bool = False) -> ProcessedRequest | None:
        """Add a request, routing it to the appropriate manager.

        Requests for explicitly configured domains are routed directly to their per-domain sub-manager. All other
        requests go to the inner manager.
        """
        url = self._get_url_from_request(request)
        domain = self._extract_domain(url)

        if domain in self._domain_states:
            sm = await self._get_or_create_sub_manager(domain)
            result = await sm.add_request(request, forefront=forefront)
        else:
            result = await self._inner.add_request(request, forefront=forefront)

        self._signal_new_work()
        return result

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
        """Add multiple requests, routing each to the appropriate manager."""
        inner_requests: list[str | Request] = []
        domain_requests: dict[str, list[str | Request]] = {}

        for request in requests:
            url = self._get_url_from_request(request)
            domain = self._extract_domain(url)

            if domain in self._domain_states:
                domain_requests.setdefault(domain, []).append(request)
            else:
                inner_requests.append(request)

        if inner_requests:
            await self._inner.add_requests(
                inner_requests,
                forefront=forefront,
                batch_size=batch_size,
                wait_time_between_batches=wait_time_between_batches,
                wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
                wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
            )

        for domain, reqs in domain_requests.items():
            sm = await self._get_or_create_sub_manager(domain)
            await sm.add_requests(
                reqs,
                forefront=forefront,
                batch_size=batch_size,
                wait_time_between_batches=wait_time_between_batches,
                wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
                wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
            )

        if inner_requests or domain_requests:
            self._signal_new_work()

    @override
    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request, respecting per-domain delays.

        Sub-managers are checked in order of longest-overdue domain first (sorted by `throttled_until` ascending). If
        all configured domains are throttled, falls back to the inner manager for non-throttled domains. If the inner
        manager is also empty and all sub-managers are throttled, waits until either the earliest domain becomes
        available or new work is added (whichever comes first).
        """
        while True:
            # Clear the event before checking the queues. Any add/reclaim that races with this iteration will set the
            # event again, so the wait at the end of the loop returns immediately rather than blocking until the
            # throttle expires.
            self._new_work_event.clear()

            now = datetime.now(timezone.utc)
            available_domains = sorted(
                (
                    domain
                    for domain, state in self._domain_states.items()
                    if domain in self._sub_managers and now >= state.throttled_until
                ),
                key=lambda d: self._domain_states[d].throttled_until,
            )

            for domain in available_domains:
                req = await self._sub_managers[domain].fetch_next_request()
                if req:
                    self._mark_domain_dispatched(domain)
                    return req

            request = await self._inner.fetch_next_request()
            if request is not None:
                return request

            if not self._sub_managers:
                return None

            sub_managers_empty = await asyncio.gather(*(sm.is_empty() for sm in self._sub_managers.values()))
            if all(sub_managers_empty):
                return None

            earliest = self._get_earliest_available_time(now)
            sleep_duration = max(
                (earliest - now).total_seconds(),
                0.1,  # Avoid tight loops if a throttle expired during the previous iteration.
            )
            logger.debug(
                f'All configured domains are throttled and inner manager is empty. '
                f'Waiting up to {sleep_duration:.1f}s for earliest domain to become available or new work.'
            )
            await self._wait_for_new_work_or_timeout(sleep_duration)

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        manager = self._select_manager(request.url)
        result = await manager.reclaim_request(request, forefront=forefront)
        self._signal_new_work()
        return result

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        manager = self._select_manager(request.url)
        result = await manager.mark_request_as_handled(request)
        self.record_success(request.url)
        return result

    @override
    async def get_handled_count(self) -> int:
        counts = await asyncio.gather(
            self._inner.get_handled_count(), *(sm.get_handled_count() for sm in self._sub_managers.values())
        )
        return sum(counts)

    @override
    async def get_total_count(self) -> int:
        counts = await asyncio.gather(
            self._inner.get_total_count(), *(sm.get_total_count() for sm in self._sub_managers.values())
        )
        return sum(counts)

    @override
    async def is_empty(self) -> bool:
        results = await asyncio.gather(self._inner.is_empty(), *(sm.is_empty() for sm in self._sub_managers.values()))
        return all(results)

    @override
    async def is_finished(self) -> bool:
        results = await asyncio.gather(
            self._inner.is_finished(), *(sm.is_finished() for sm in self._sub_managers.values())
        )
        return all(results)

    def record_domain_delay(self, url: str, *, retry_after: timedelta | None = None) -> bool:
        """Record a 429 Too Many Requests response for the domain of the given URL.

        Increments the consecutive 429 count and calculates the next allowed request time using exponential backoff or
        the `Retry-After` value.

        Args:
            url: The URL that received a 429 response.
            retry_after: Optional delay from the `Retry-After` header. If provided, it takes priority over the
                calculated exponential backoff.

        Returns:
            True if the URL's domain is configured for throttling and the delay was applied; False if the domain is not
            in the configured `domains` list, in which case the call is a no-op.
        """
        state = self._get_domain_state(url)
        if state is None:
            return False

        state.consecutive_429_count += 1
        delay = retry_after if retry_after is not None else self._base_delay * (2 ** (state.consecutive_429_count - 1))
        if delay > self._max_delay:
            source = 'Retry-After header' if retry_after is not None else 'exponential backoff'
            logger.warning(
                f'Capping {source} delay of {delay.total_seconds():.1f}s for domain "{state.domain}" '
                f'to max_delay ({self._max_delay.total_seconds():.1f}s); the domain may continue to rate-limit. '
                f'Consider increasing max_delay if this recurs.'
            )
            delay = self._max_delay
        state.throttled_until = datetime.now(timezone.utc) + delay

        logger.info(
            f'Rate limit (429) detected for domain "{state.domain}" '
            f'(consecutive: {state.consecutive_429_count}, delay: {delay.total_seconds():.1f}s)'
        )
        return True

    def record_success(self, url: str) -> None:
        """Record a successful request, resetting the backoff state for that domain.

        Args:
            url: The URL that received a successful response.
        """
        state = self._get_domain_state(url)
        if state is not None and state.consecutive_429_count > 0:
            logger.debug(f'Resetting rate limit state for domain "{state.domain}" after successful request')
            state.consecutive_429_count = 0

    def set_crawl_delay(self, url: str, delay_seconds: int) -> None:
        """Set the robots.txt crawl-delay for a domain.

        The delay is locked once set so robots.txt re-fetches (e.g. after LRU eviction) can't change the in-flight
        dispatch cadence and cause oscillation mid-crawl. Subsequent calls for the same domain are no-ops.

        Args:
            url: A URL from the domain to throttle.
            delay_seconds: The crawl-delay value in seconds.
        """
        state = self._get_domain_state(url)
        if state is None or state.crawl_delay is not None:
            return

        state.crawl_delay = timedelta(seconds=delay_seconds)
        logger.debug(f'Set crawl-delay for domain "{state.domain}" to {delay_seconds}s')

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract the domain (hostname) from a URL."""
        return URL(url).host or ''

    @staticmethod
    def _get_url_from_request(request: str | Request) -> str:
        """Extract URL string from a request that may be a string or Request object."""
        return request if isinstance(request, str) else request.url

    def _get_domain_state(self, url: str) -> _DomainState | None:
        """Look up the per-domain state for the given URL, if the domain is configured."""
        domain = self._extract_domain(url)
        return self._domain_states.get(domain) if domain else None

    async def _get_or_create_sub_manager(self, domain: str) -> TRequestManager:
        """Get or create a per-domain sub-manager using the configured `request_manager_opener`."""
        if domain not in self._sub_managers:
            self._sub_managers[domain] = await self._request_manager_opener(
                alias=f'throttled-{domain}',
                storage_client=self._service_locator.get_storage_client(),
                configuration=self._service_locator.get_configuration(),
            )
        return self._sub_managers[domain]

    def _is_domain_throttled(self, domain: str) -> bool:
        """Check if a domain is currently throttled."""
        state = self._domain_states.get(domain)
        if state is None:
            return False
        return datetime.now(timezone.utc) < state.throttled_until

    def _get_earliest_available_time(self, now: datetime) -> datetime:
        """Get the earliest time any throttled domain becomes available."""
        earliest = now + self._max_delay

        for state in self._domain_states.values():
            if now < state.throttled_until < earliest:
                earliest = state.throttled_until

        return earliest

    def _mark_domain_dispatched(self, domain: str) -> None:
        """Record that a request to this domain was just dispatched.

        If a crawl-delay is configured, push throttled_until forward by that amount.
        """
        state = self._domain_states.get(domain)
        if state is not None and state.crawl_delay is not None:
            state.throttled_until = datetime.now(timezone.utc) + state.crawl_delay

    def _signal_new_work(self) -> None:
        """Wake `fetch_next_request` if it is sleeping inside a throttle wait."""
        self._new_work_event.set()

    def _select_manager(self, url: str) -> TRequestManager:
        """Return the manager that owns the given URL — its sub-manager if one exists, otherwise the inner."""
        domain = self._extract_domain(url)
        if domain in self._sub_managers:
            return self._sub_managers[domain]
        return self._inner

    async def _wait_for_new_work_or_timeout(self, timeout: float) -> None:
        """Wait until new work is signaled or `timeout` seconds elapse, whichever comes first.

        The signal is set by `add_request`, `add_requests`, and `reclaim_request`, allowing `fetch_next_request` to wake
        up immediately when fresh work appears during a throttle wait instead of sleeping for the full computed cooldown
        (up to `max_delay`).
        """
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._new_work_event.wait(), timeout=timeout)


class _RequestManagerOpener(Protocol[TRequestManager]):
    """Callable that opens a `RequestManager` instance.

    Matches the keyword-only signature shared by storage `open` classmethods such as `RequestQueue.open`.
    `ThrottlingRequestManager` invokes the opener at sub-manager creation time, so every sub-manager shares the same
    backing type as `inner`.
    """

    async def __call__(
        self,
        *,
        alias: str | None = ...,
        storage_client: StorageClient | None = ...,
        configuration: Configuration | None = ...,
    ) -> TRequestManager: ...


@dataclass
class _DomainState:
    """Tracks delay state for a single domain."""

    domain: str
    """The domain being tracked."""

    throttled_until: datetime = _NEVER_THROTTLED
    """Earliest time the next request to this domain is allowed."""

    consecutive_429_count: int = 0
    """Number of consecutive 429 responses (for exponential backoff)."""

    crawl_delay: timedelta | None = None
    """Minimum interval between requests, used to push `throttled_until` on dispatch."""
