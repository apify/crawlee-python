from __future__ import annotations

import asyncio
import ipaddress
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
"""Sentinel timestamp meaning one of a domain's throttle clocks has never been armed."""

_MAX_BACKOFF_EXPONENT = 20
"""Highest exponent the 429 backoff doubles to. `max_delay` caps the delay far below this, while an unbounded exponent
eventually overflows the `timedelta` multiplication. Low enough that the doubling stays representable for any
`base_delay` up to a year.
"""


@docs_group('Request loaders')
class ThrottlingRequestManager(RequestManager, Generic[TRequestManager]):
    """A request manager that wraps another and enforces per-domain delays.

    Requests for explicitly configured domains are routed into dedicated sub-managers, so each request lives in exactly
    one store and is deduplicated there. A request that reached `inner` before its domain was configured stays and is
    completed there, without the domain's delay.

    `fetch_next_request()` takes from the sub-manager whose domain has been waiting the longest, skipping domains in a
    cooldown, and falls back to the inner manager when no sub-manager yields a request. If nothing can be dispatched
    right now, it returns `None` rather than waiting, so the caller's task slot is released. `is_empty()` reports the
    same view and reads as empty while every remaining request sits in a cooldown, whereas `is_finished()` counts those
    requests, so the crawl idles until they are dispatchable instead of ending early.

    Delay sources:
    - HTTP 429 responses (via `record_domain_delay`)
    - robots.txt crawl-delay directives (via `set_crawl_delay`)

    The class is generic over the wrapped manager type. The first asynchronous operation opens one sub-manager per
    configured domain through `request_manager_opener`, so all of them share the subclass and backing store of `inner`;
    the synchronous delay methods never open anything. The opener must accept `alias`, `storage_client`, and
    `configuration` keyword arguments (as `RequestQueue.open` does) and return the same concrete subclass as `inner`.

    Requests a previous run left in a persistent store become visible again at open. The default `purge_on_start=True`
    empties them; `purge_on_start=False` resumes them. Named stores are exempt from that purge and aliased ones are not,
    so a named `inner` keeps its requests while the per-domain stores are emptied.

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
            domains: Domains to throttle, each given as a bare hostname such as `api.example.com`, or as any URL on
                the domain, of which only the hostname is used. Blank entries are ignored, so a list built by
                splitting a string needs no pruning. Only requests matching these domains will be routed to
                per-domain sub-managers. Matching is exact but spelling-insensitive: casing, punycode versus Unicode,
                and a trailing root dot are all normalized away. Subdomain wildcards such as `*.example.com` are not
                supported — list each subdomain explicitly if needed.
            request_manager_opener: Async callable used to open one sub-manager per configured domain on first use.
                Must accept `alias`, `storage_client`, and `configuration` keyword arguments and return the same
                concrete subclass as `inner` (e.g. `RequestQueue.open` when `inner` is a `RequestQueue`).
            service_locator: Service locator for creating sub-managers. If not provided, defaults to the global service
                locator, ensuring consistency with the crawler's storage backend.
            base_delay: Initial delay after the first 429 response from a domain.
            max_delay: Maximum delay between requests to a rate-limited domain.

        Raises:
            ValueError: If a non-blank entry of `domains` does not yield a hostname a crawled URL could match.
        """
        self._inner: TRequestManager = inner
        self._service_locator = service_locator if service_locator is not None else global_service_locator
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._request_manager_opener = request_manager_opener
        # Padding on an entry would otherwise survive parsing into a key no crawled hostname can match.
        domain_keys = [self._parse_configured_domain(entry) for d in domains if (entry := d.strip())]
        self._domain_states: dict[str, _DomainState] = {key: _DomainState(domain=key) for key in domain_keys}
        self._sub_managers: dict[str, TRequestManager] = {}
        self._sub_managers_ready = False
        self._sub_managers_lock = asyncio.Lock()
        self._in_flight_from_inner: set[tuple[str, str]] = set()
        """`(unique_key, url)` pairs of configured-domain requests that `fetch_next_request` took from `inner`, where
        they live if they were added before their domain was listed, and where they must be completed. The URL is part
        of the key because an explicit `unique_key` is only unique per store. Identical pairs held by `inner` and by a
        sub-manager are indistinguishable, so their completions can cross; both stores hold the key, so the cost is a
        duplicate crawl and a retry without the domain's delay."""

    @property
    def inner(self) -> TRequestManager:
        """The wrapped request manager that stores requests for non-throttled domains."""
        return self._inner

    @override
    async def drop(self) -> None:
        await self._ensure_sub_managers()
        await asyncio.gather(self._inner.drop(), *(sm.drop() for sm in self._sub_managers.values()))
        self._sub_managers.clear()
        self._sub_managers_ready = False
        self._in_flight_from_inner.clear()

    @override
    async def purge(self) -> None:
        """Empty the inner manager and all sub-managers, and reset transient per-domain throttle state.

        The configured domain list and any robots.txt-derived `crawl_delay` are preserved. Only the dynamic backoff
        state (consecutive 429 counter and the throttle clocks) is cleared. Sub-managers stay open; they're just
        emptied.
        """
        await self._ensure_sub_managers()
        await asyncio.gather(self._inner.purge(), *(sm.purge() for sm in self._sub_managers.values()))
        self._in_flight_from_inner.clear()
        for state in self._domain_states.values():
            state.reset_throttling()

    @override
    async def add_request(self, request: str | Request, *, forefront: bool = False) -> ProcessedRequest | None:
        """Add a request, routing it to the appropriate manager.

        Requests for explicitly configured domains are routed directly to their per-domain sub-manager. All other
        requests go to the inner manager.
        """
        await self._ensure_sub_managers()

        url = self._get_url_from_request(request)
        domain = self._extract_domain(url)

        if domain in self._domain_states:
            return await self._sub_managers[domain].add_request(request, forefront=forefront)

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
        """Add multiple requests, routing each to the appropriate manager."""
        await self._ensure_sub_managers()

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
            await self._sub_managers[domain].add_requests(
                reqs,
                forefront=forefront,
                batch_size=batch_size,
                wait_time_between_batches=wait_time_between_batches,
                wait_for_all_requests_to_be_added=wait_for_all_requests_to_be_added,
                wait_for_all_requests_to_be_added_timeout=wait_for_all_requests_to_be_added_timeout,
            )

    @override
    async def fetch_next_request(self) -> Request | None:
        """Fetch the next request, respecting per-domain delays.

        Sub-managers are checked in order of longest-overdue domain first, then the inner manager. Domains in a
        cooldown are skipped, so the call returns `None` when nothing is dispatchable right now.

        Note:
            Unlike the `RequestLoader.fetch_next_request` contract, a `None` result does not imply that `is_finished()`
            is `True` - it only means nothing is dispatchable right now. Since the manager never waits out a cooldown
            itself, the dispatch cadence is only as precise as the caller's polling interval: a cooldown expiring
            between two polls is picked up on the next one.
        """
        await self._ensure_sub_managers()

        for domain in self._fetchable_domains():
            request = await self._sub_managers[domain].fetch_next_request()
            if request is not None:
                self._mark_domain_dispatched(domain)
                return request

        request = await self._inner.fetch_next_request()
        if request is not None and self._extract_domain(request.url) in self._domain_states:
            self._in_flight_from_inner.add((request.unique_key, request.url))
        return request

    @override
    async def reclaim_request(self, request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        await self._ensure_sub_managers()
        manager = self._fetch_owner(request)
        result = await manager.reclaim_request(request, forefront=forefront)
        self._clear_fetch_owner(request)
        return result

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        await self._ensure_sub_managers()
        manager = self._fetch_owner(request)
        result = await manager.mark_request_as_handled(request)
        self._clear_fetch_owner(request)
        return result

    @override
    async def get_handled_count(self) -> int:
        await self._ensure_sub_managers()
        counts = await asyncio.gather(
            self._inner.get_handled_count(), *(sm.get_handled_count() for sm in self._sub_managers.values())
        )
        return sum(counts)

    @override
    async def get_total_count(self) -> int:
        await self._ensure_sub_managers()
        counts = await asyncio.gather(
            self._inner.get_total_count(), *(sm.get_total_count() for sm in self._sub_managers.values())
        )
        return sum(counts)

    @override
    async def is_empty(self) -> bool:
        """Report whether anything can be dispatched right now.

        Requests queued for a domain in a cooldown do not count. They still count towards `is_finished`, so the crawl
        waits for them.
        """
        await self._ensure_sub_managers()
        results = await asyncio.gather(
            self._inner.is_empty(), *(self._sub_managers[d].is_empty() for d in self._fetchable_domains())
        )
        return all(results)

    @override
    async def is_finished(self) -> bool:
        await self._ensure_sub_managers()
        results = await asyncio.gather(
            self._inner.is_finished(), *(sm.is_finished() for sm in self._sub_managers.values())
        )
        return all(results)

    def record_domain_delay(self, url: str, *, retry_after: timedelta | None = None) -> bool:
        """Record a 429 Too Many Requests response for the domain of the given URL.

        Advances the consecutive 429 count and calculates the next allowed request time using exponential backoff or
        the `Retry-After` value. Only the first 429 of a burst advances the count, so the delay tracks how hard the
        domain pushes back, not how many requests were in flight.

        Args:
            url: The URL that received a 429 response.
            retry_after: Optional delay from the `Retry-After` header. If it describes a positive delay, it takes
                priority over the calculated exponential backoff.

        Returns:
            True if the URL's domain is configured for throttling, whether or not this 429 advanced the backoff; False
            if the domain is not in the configured `domains` list, in which case the call is a no-op.
        """
        state = self._get_domain_state(url)
        if state is None:
            return False

        now = datetime.now(timezone.utc)

        # Requests in flight when the limit was hit all come back 429. That is one rate-limit event, so only the first
        # advances the exponent. Checking `crawl_delay_until` too would swallow every 429, as it is armed on every
        # dispatch.
        if now < state.backoff_until:
            logger.debug(
                f'Ignoring an HTTP 429 from domain "{state.domain}" received during an active backoff '
                f'(consecutive: {state.consecutive_429_count}).'
            )
            return True

        # The domain has been quiet for a full extra window, so this 429 opens a new run instead of continuing the old.
        if now >= state.backoff_decays_at:
            state.consecutive_429_count = 0

        state.consecutive_429_count += 1

        # A non-positive `Retry-After` is no delay at all, so fall back to the backoff and let it engage.
        if retry_after is not None and retry_after > timedelta(0):
            delay = retry_after
            source = 'Retry-After header'
        else:
            delay = self._base_delay * 2 ** min(state.consecutive_429_count - 1, _MAX_BACKOFF_EXPONENT)
            source = 'exponential backoff'

        if delay > self._max_delay:
            logger.warning(
                f'Capping {source} delay of {delay.total_seconds():.1f}s for domain "{state.domain}" '
                f'to max_delay ({self._max_delay.total_seconds():.1f}s); the domain may continue to rate-limit. '
                f'Consider increasing max_delay if this recurs.'
            )
            delay = self._max_delay

        state.apply_backoff(now, delay)

        logger.info(
            f'Rate limit (429) detected for domain "{state.domain}" '
            f'(consecutive: {state.consecutive_429_count}, delay: {delay.total_seconds():.1f}s)'
        )
        return True

    def record_success(self, url: str) -> None:
        """Reset a domain's consecutive 429 count, so the next 429 starts the backoff over at `base_delay`.

        An active backoff window is not lifted. The manager does not call this itself; the count decays on its own once
        the domain has stopped rate-limiting for a full extra window.

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
    def _normalize_domain(hostname: str) -> str:
        """Reduce a parsed hostname to the form domain keys are stored in: lowercase, without the root dot."""
        return hostname.lower().removesuffix('.')

    @classmethod
    def _parse_configured_domain(cls, domain: str) -> str:
        """Turn one `domains` entry, a bare hostname or a URL, into the key its requests are looked up under."""
        if '://' in domain:
            url_text = domain
        else:
            # A bare hostname reaches the parser's IDNA handling only through a synthetic URL, and a bare IPv6
            # literal has to be bracketed there, or the parser reads its last group as a port.
            try:
                ipaddress.IPv6Address(domain)
            except ValueError:
                url_text = f'https://{domain}'
            else:
                url_text = f'https://[{domain}]'

        try:
            host = URL(url_text).host
        except ValueError:
            host = None

        key = cls._normalize_domain(host) if host else ''

        # A wildcard passes through the parser untouched, so it would become a key no crawled hostname can match.
        if not key or '*' in key:
            raise ValueError(
                f'"{domain}" is not a valid hostname. The `domains` option takes bare hostnames such as '
                '"example.com", or any URL on the domain.'
            )

        return key

    @classmethod
    def _extract_domain(cls, url: str) -> str:
        """Extract the domain key from a URL."""
        return cls._normalize_domain(URL(url).host or '')

    @staticmethod
    def _get_url_from_request(request: str | Request) -> str:
        """Extract URL string from a request that may be a string or Request object."""
        return request if isinstance(request, str) else request.url

    def _get_domain_state(self, url: str) -> _DomainState | None:
        """Look up the per-domain state for the given URL, if the domain is configured."""
        domain = self._extract_domain(url)
        return self._domain_states.get(domain) if domain else None

    async def _open_sub_manager(self, domain: str) -> None:
        """Open the sub-manager for a single domain using the configured `request_manager_opener`."""
        self._sub_managers[domain] = await self._request_manager_opener(
            alias=f'throttled-{domain}',
            storage_client=self._service_locator.get_storage_client(),
            configuration=self._service_locator.get_configuration(),
        )

    async def _ensure_sub_managers(self) -> None:
        """Open a sub-manager for every configured domain, once; a retry opens only what is still missing."""
        if self._sub_managers_ready:
            return

        async with self._sub_managers_lock:
            if self._sub_managers_ready:
                return

            # All attempts must settle before the lock is released: openers left running would write into
            # `_sub_managers` after a retry has already replaced that domain's manager.
            missing = [domain for domain in self._domain_states if domain not in self._sub_managers]
            results = await asyncio.gather(
                *(self._open_sub_manager(domain) for domain in missing), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result

            self._sub_managers_ready = True

    def _is_domain_throttled(self, domain: str) -> bool:
        """Check if a domain is currently throttled."""
        state = self._domain_states.get(domain)
        if state is None:
            return False
        return datetime.now(timezone.utc) < state.throttled_until

    def _fetchable_domains(self) -> list[str]:
        """Return the configured domains that are not in a cooldown right now, longest-overdue first."""
        now = datetime.now(timezone.utc)
        available = [domain for domain, state in self._domain_states.items() if now >= state.throttled_until]
        available.sort(key=lambda domain: self._domain_states[domain].throttled_until)
        return available

    def _mark_domain_dispatched(self, domain: str) -> None:
        """Record that a request to this domain was just dispatched.

        If a crawl-delay is configured, push `crawl_delay_until` forward by that amount.
        """
        state = self._domain_states.get(domain)
        if state is not None:
            state.apply_crawl_delay(datetime.now(timezone.utc))

    def _fetch_owner(self, request: Request) -> TRequestManager:
        """Return the manager the request must be given back to, leaving its in-flight record in place.

        `_clear_fetch_owner` drops the record only once the completion is accepted, so a retry resolves the same way.
        """
        if (request.unique_key, request.url) in self._in_flight_from_inner:
            return self._inner
        return self._sub_managers.get(self._extract_domain(request.url), self._inner)

    def _clear_fetch_owner(self, request: Request) -> None:
        """Drop the in-flight record of a request whose completion was accepted."""
        self._in_flight_from_inner.discard((request.unique_key, request.url))


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

    backoff_until: datetime = _NEVER_THROTTLED
    """Earliest time the next request is allowed by the 429 backoff. Kept apart from `crawl_delay_until`, which is
    armed on every dispatch and would otherwise pass for an active backoff.
    """

    crawl_delay_until: datetime = _NEVER_THROTTLED
    """Earliest time the next request is allowed by the domain's crawl-delay."""

    backoff_decays_at: datetime = _NEVER_THROTTLED
    """Time after which an incoming 429 is treated as a fresh burst rather than a continuation of the current one."""

    consecutive_429_count: int = 0
    """Number of consecutive 429 responses (for exponential backoff)."""

    crawl_delay: timedelta | None = None
    """Minimum interval between requests, used to push `crawl_delay_until` on dispatch."""

    @property
    def throttled_until(self) -> datetime:
        """Earliest time the next request to this domain is allowed by either of its two independent clocks."""
        return max(self.backoff_until, self.crawl_delay_until)

    def apply_backoff(self, now: datetime, delay: timedelta) -> None:
        """Block the domain for `delay`.

        If no 429 arrives for another `delay` after the domain becomes dispatchable again, the exponent resets.
        """
        self.backoff_until = now + delay
        # The quiet period runs from the moment the domain becomes dispatchable again, not from `backoff_until`. A
        # crawl-delay longer than `delay` sets the retry cadence, and measuring from `backoff_until` would expire the
        # window before the domain is even retried, making every 429 look like a fresh burst.
        self.backoff_decays_at = self.throttled_until + delay

    def apply_crawl_delay(self, now: datetime) -> None:
        """Block the domain for its crawl-delay, if it declared one."""
        if self.crawl_delay is not None:
            self.crawl_delay_until = now + self.crawl_delay

    def reset_throttling(self) -> None:
        """Clear the transient throttle state."""
        self.consecutive_429_count = 0
        self.backoff_until = _NEVER_THROTTLED
        self.crawl_delay_until = _NEVER_THROTTLED
        self.backoff_decays_at = _NEVER_THROTTLED
