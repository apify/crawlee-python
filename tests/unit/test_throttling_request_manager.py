"""Tests for ThrottlingRequestManager - per-domain delay scheduling."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from crawlee._request import Request
from crawlee._service_locator import ServiceLocator
from crawlee._utils.http import parse_retry_after_header
from crawlee.request_loaders._throttling_request_manager import ThrottlingRequestManager
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

THROTTLED_DOMAIN = 'throttled.com'
SECOND_THROTTLED_DOMAIN = 'slow.com'
NON_THROTTLED_DOMAIN = 'free.com'
TEST_DOMAINS = [THROTTLED_DOMAIN]


@pytest.fixture
def memory_storage_client() -> MemoryStorageClient:
    """Create a MemoryStorageClient for testing."""
    return MemoryStorageClient()


@pytest.fixture
def service_locator(memory_storage_client: MemoryStorageClient) -> ServiceLocator:
    """Create a ServiceLocator with MemoryStorageClient for testing."""
    return ServiceLocator(storage_client=memory_storage_client)


@pytest.fixture
async def inner_queue(memory_storage_client: MemoryStorageClient) -> RequestQueue:
    """Create a real RequestQueue with MemoryStorageClient."""
    return await RequestQueue.open(name='test-inner', storage_client=memory_storage_client)


@pytest.fixture
async def manager(inner_queue: RequestQueue, service_locator: ServiceLocator) -> ThrottlingRequestManager[RequestQueue]:
    """Create a ThrottlingRequestManager wrapping a real queue with test domains."""
    return ThrottlingRequestManager(
        inner_queue,
        domains=TEST_DOMAINS,
        request_manager_opener=RequestQueue.open,
        service_locator=service_locator,
    )


@pytest.fixture
async def two_domain_manager(
    inner_queue: RequestQueue,
    service_locator: ServiceLocator,
) -> ThrottlingRequestManager[RequestQueue]:
    """Create a ThrottlingRequestManager with two throttled domains."""
    return ThrottlingRequestManager(
        inner_queue,
        domains=[THROTTLED_DOMAIN, SECOND_THROTTLED_DOMAIN],
        request_manager_opener=RequestQueue.open,
        service_locator=service_locator,
    )


def _make_request(url: str) -> Request:
    """Helper to create a Request object."""
    return Request.from_url(url)


# ── Request Routing Tests ─────────────────────────────────


async def test_add_request_routes_listed_domain_to_sub_manager(
    manager: ThrottlingRequestManager[RequestQueue],
    inner_queue: RequestQueue,
) -> None:
    """Requests for listed domains should be routed to their sub-manager, not inner."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    await manager.add_request(request)

    # Inner manager should be empty — the request went to a sub-manager.
    assert await inner_queue.is_empty()
    assert THROTTLED_DOMAIN in manager._sub_managers

    # The sub-manager should have the request.
    assert await manager._sub_managers[THROTTLED_DOMAIN].get_total_count() == 1


async def test_add_request_routes_non_listed_domain_to_inner(
    manager: ThrottlingRequestManager[RequestQueue],
    inner_queue: RequestQueue,
) -> None:
    """Requests for non-listed domains should go to the inner manager."""
    request = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')
    await manager.add_request(request)

    assert not await inner_queue.is_empty()
    assert NON_THROTTLED_DOMAIN not in manager._sub_managers


async def test_add_request_with_string_url(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """add_request should also work when given a plain URL string."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    assert THROTTLED_DOMAIN in manager._sub_managers
    assert await manager._sub_managers[THROTTLED_DOMAIN].get_total_count() == 1


async def test_domain_matching_is_case_insensitive(
    inner_queue: RequestQueue,
    service_locator: ServiceLocator,
) -> None:
    """Domains with mixed casing should match URLs whose hostnames yarl lowercases."""
    manager = ThrottlingRequestManager(
        inner_queue,
        domains=['Example.COM'],
        request_manager_opener=RequestQueue.open,
        service_locator=service_locator,
    )

    await manager.add_request('https://EXAMPLE.com/page1')

    assert await inner_queue.is_empty()
    assert 'example.com' in manager._sub_managers
    assert await manager._sub_managers['example.com'].get_total_count() == 1

    # 429 bookkeeping must also flow through the lowercased state.
    manager.record_domain_delay('https://Example.Com/page2')
    assert manager._is_domain_throttled('example.com')


@pytest.mark.parametrize(
    ('configured', 'url'),
    [
        pytest.param('xn--hky-ela4t.cz', 'https://háčky.cz/page', id='punycode entry'),
        pytest.param('háčky.cz', 'https://xn--hky-ela4t.cz/page', id='punycode url'),
        pytest.param('example.com', 'http://example.com./page', id='root dot in url'),
        pytest.param('example.com.', 'http://example.com/page', id='root dot in entry'),
        pytest.param('[::1]', 'http://[::1]:8080/page', id='bracketed ipv6'),
        pytest.param('::1', 'http://[::1]:8080/page', id='bare ipv6'),
        pytest.param('https://example.com/products', 'https://example.com/page', id='full url'),
        pytest.param('example.com:8080/path:1', 'https://example.com:8080/page', id='scheme-less url with colons'),
        pytest.param(' example.com ', 'https://example.com/page', id='padded entry'),
    ],
)
async def test_domain_matching_normalizes_spelling(
    configured: str,
    url: str,
    inner_queue: RequestQueue,
    service_locator: ServiceLocator,
) -> None:
    """A configured domain and a crawled URL must land on the same key however each of them is spelled."""
    manager = ThrottlingRequestManager(
        inner_queue,
        domains=[configured],
        request_manager_opener=RequestQueue.open,
        service_locator=service_locator,
    )

    assert manager.record_domain_delay(url) is True


@pytest.mark.parametrize(
    'configured',
    [
        pytest.param('.', id='bare root dot'),
        pytest.param('[::1', id='unclosed ipv6 bracket'),
        pytest.param('*.example.com', id='subdomain wildcard'),
        pytest.param('example.com:8080:9090', id='stray colons'),
    ],
)
async def test_unmatchable_domain_is_rejected(
    configured: str,
    inner_queue: RequestQueue,
    service_locator: ServiceLocator,
) -> None:
    """An entry that cannot yield a matchable hostname is rejected at construction, not silently kept."""
    with pytest.raises(ValueError, match='not a valid hostname'):
        ThrottlingRequestManager(
            inner_queue,
            domains=[configured],
            request_manager_opener=RequestQueue.open,
            service_locator=service_locator,
        )


async def test_blank_domain_entries_are_ignored(
    inner_queue: RequestQueue,
    service_locator: ServiceLocator,
) -> None:
    """Blank entries are dropped rather than rejected, so a list built by splitting a string needs no pruning."""
    manager = ThrottlingRequestManager(
        inner_queue,
        domains=['', '   ', 'example.com'],
        request_manager_opener=RequestQueue.open,
        service_locator=service_locator,
    )

    assert set(manager._domain_states) == {'example.com'}


async def test_add_requests_routes_mixed_domains(
    manager: ThrottlingRequestManager[RequestQueue],
    inner_queue: RequestQueue,
) -> None:
    """add_requests should split requests by domain and route them correctly."""
    throttled_req = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    free_req = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')

    await manager.add_requests([throttled_req, free_req])

    # Inner gets the non-listed domain request.
    assert not await inner_queue.is_empty()

    # Sub-manager gets the listed domain request.
    assert THROTTLED_DOMAIN in manager._sub_managers
    assert await manager._sub_managers[THROTTLED_DOMAIN].get_total_count() == 1


# ── Core Throttling Tests ─────────────────────────────────


async def test_429_triggers_domain_delay(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """After record_domain_delay(), the domain should be throttled and the call should report success."""
    assert manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1') is True
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)


async def test_record_domain_delay_returns_false_for_unconfigured_domain(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """record_domain_delay should return False (no-op) when the URL's domain is not in the configured list."""
    assert manager.record_domain_delay(f'https://{NON_THROTTLED_DOMAIN}/page1') is False
    assert not manager._is_domain_throttled(NON_THROTTLED_DOMAIN)


async def test_different_domains_independent(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """Throttling one domain should NOT affect other domains."""
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1')
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)
    assert not manager._is_domain_throttled(NON_THROTTLED_DOMAIN)


async def test_exponential_backoff(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """Consecutive 429s should increase delay exponentially."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url)
    state = manager._domain_states[THROTTLED_DOMAIN]
    first_until = state.throttled_until

    manager.record_domain_delay(url)
    second_until = state.throttled_until

    assert second_until > first_until
    assert state.consecutive_429_count == 2


async def test_max_delay_cap(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """Backoff should cap at max_delay (60s)."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    for _ in range(20):
        manager.record_domain_delay(url)

    state = manager._domain_states[THROTTLED_DOMAIN]
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay <= manager._max_delay + timedelta(seconds=1)


async def test_retry_after_header_priority(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """Explicit Retry-After should override exponential backoff."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url, retry_after=timedelta(seconds=30))

    state = manager._domain_states[THROTTLED_DOMAIN]
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay > timedelta(seconds=28)
    assert actual_delay <= timedelta(seconds=31)


async def test_retry_after_exceeding_max_delay_logs_warning(
    manager: ThrottlingRequestManager[RequestQueue],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A Retry-After value above max_delay should still be capped, but emit a warning."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    with caplog.at_level('WARNING', logger='crawlee.request_loaders._throttling_request_manager'):
        manager.record_domain_delay(url, retry_after=manager._max_delay + timedelta(seconds=240))

    state = manager._domain_states[THROTTLED_DOMAIN]
    actual_delay = state.throttled_until - datetime.now(timezone.utc)
    assert actual_delay <= manager._max_delay + timedelta(seconds=1)

    warnings = [r for r in caplog.records if r.levelname == 'WARNING']
    assert len(warnings) == 1
    assert 'Retry-After header' in warnings[0].message
    assert THROTTLED_DOMAIN in warnings[0].message


async def test_success_resets_backoff(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """Successful request should reset the consecutive 429 count."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url)
    manager.record_domain_delay(url)
    assert manager._domain_states[THROTTLED_DOMAIN].consecutive_429_count == 2

    manager.record_success(url)
    assert manager._domain_states[THROTTLED_DOMAIN].consecutive_429_count == 0


# ── Crawl-Delay Integration Tests ─────────────────────────


async def test_crawl_delay_integration(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """set_crawl_delay() should record the delay for the domain."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    manager.set_crawl_delay(url, 5)

    state = manager._domain_states[THROTTLED_DOMAIN]
    assert state.crawl_delay == timedelta(seconds=5)


async def test_crawl_delay_throttles_after_dispatch(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """After dispatching a request, crawl-delay should throttle the next one."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    manager.set_crawl_delay(url, 5)

    manager._mark_domain_dispatched(THROTTLED_DOMAIN)

    assert manager._is_domain_throttled(THROTTLED_DOMAIN)


# ── Fetch Scheduling Tests ────────────────────────────


async def test_fetch_from_unthrottled_sub_manager(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """fetch_next_request should return from an unthrottled sub-manager."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == url


async def test_fetch_falls_back_to_inner(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """When sub-managers are empty, should return from inner manager."""
    url = f'https://{NON_THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == url


async def test_fetch_skips_throttled_sub_manager(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """Should skip throttled sub-managers and fall through to inner."""
    # Add a request to the throttled domain and mark it as throttled.
    throttled_url = f'https://{THROTTLED_DOMAIN}/page1'
    free_url = f'https://{NON_THROTTLED_DOMAIN}/page1'

    await manager.add_request(throttled_url)
    await manager.add_request(free_url)

    manager.record_domain_delay(throttled_url)

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == free_url


async def test_fetch_returns_none_when_all_throttled(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """A throttled domain must release the caller's concurrency slot instead of waiting out its cooldown."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)
    manager.record_domain_delay(url, retry_after=timedelta(seconds=60))

    assert await asyncio.wait_for(manager.fetch_next_request(), timeout=1.0) is None


async def test_throttled_domain_reads_as_empty_but_not_finished(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """Requests waiting out a cooldown are nothing to dispatch, but the crawl must not end on them either."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)
    manager.record_domain_delay(url, retry_after=timedelta(seconds=60))

    assert await manager.is_empty() is True
    assert await manager.is_finished() is False


async def test_crawl_delay_hides_queued_requests(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """A crawl-delay armed on dispatch keeps the domain's remaining requests out of `is_empty`."""
    manager.set_crawl_delay(f'https://{THROTTLED_DOMAIN}/', 30)
    await manager.add_request(f'https://{THROTTLED_DOMAIN}/page1')
    await manager.add_request(f'https://{THROTTLED_DOMAIN}/page2')

    assert await manager.fetch_next_request() is not None

    assert await manager.is_empty() is True
    assert await manager.is_finished() is False


async def test_expired_throttle_makes_the_domain_fetchable_again(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """Once the cooldown passes, the domain counts again for both dispatching and emptiness."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)
    manager.record_domain_delay(url, retry_after=timedelta(seconds=60))
    assert await manager.is_empty() is True

    manager._domain_states[THROTTLED_DOMAIN].throttled_until = datetime.now(timezone.utc) - timedelta(seconds=1)

    assert await manager.is_empty() is False
    result = await manager.fetch_next_request()
    assert result is not None
    assert result.url == url


async def test_fetch_prefers_longest_overdue_domain(
    two_domain_manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """With several domains free, the one whose cooldown expired earliest is dispatched first."""
    recent_url = f'https://{THROTTLED_DOMAIN}/page1'
    overdue_url = f'https://{SECOND_THROTTLED_DOMAIN}/page1'
    await two_domain_manager.add_request(recent_url)
    await two_domain_manager.add_request(overdue_url)

    now = datetime.now(timezone.utc)
    two_domain_manager._domain_states[THROTTLED_DOMAIN].throttled_until = now - timedelta(seconds=1)
    two_domain_manager._domain_states[SECOND_THROTTLED_DOMAIN].throttled_until = now - timedelta(seconds=10)

    result = await two_domain_manager.fetch_next_request()

    assert result is not None
    assert result.url == overdue_url


# ── Delegation Tests ────────────────────────────────────


async def test_reclaim_request_routes_to_sub_manager(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """reclaim_request should route to sub-manager for listed domains."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    # Fetch it first, then reclaim.
    request = await manager.fetch_next_request()
    assert request is not None

    await manager.reclaim_request(request)

    # Should be back in the sub-manager.
    assert not await manager._sub_managers[THROTTLED_DOMAIN].is_empty()


async def test_reclaim_request_routes_to_inner(
    manager: ThrottlingRequestManager[RequestQueue],
    inner_queue: RequestQueue,
) -> None:
    """reclaim_request should route to inner for non-listed domains."""
    url = f'https://{NON_THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    request = await manager.fetch_next_request()
    assert request is not None

    await manager.reclaim_request(request)

    # Should be back in inner manager.
    assert not await inner_queue.is_empty()


async def test_mark_request_as_handled_routes_to_sub_manager(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """mark_request_as_handled should route to sub-manager for listed domains."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    request = await manager.fetch_next_request()
    assert request is not None

    await manager.mark_request_as_handled(request)

    assert await manager._sub_managers[THROTTLED_DOMAIN].get_handled_count() == 1


async def test_mark_request_as_handled_routes_to_inner(
    manager: ThrottlingRequestManager[RequestQueue],
    inner_queue: RequestQueue,
) -> None:
    """mark_request_as_handled should route to inner for non-listed domains."""
    url = f'https://{NON_THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    request = await manager.fetch_next_request()
    assert request is not None

    await manager.mark_request_as_handled(request)

    assert await inner_queue.get_handled_count() == 1


async def test_get_handled_count_aggregates(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """get_handled_count should sum inner and all sub-managers."""
    throttled_url = f'https://{THROTTLED_DOMAIN}/page1'
    free_url = f'https://{NON_THROTTLED_DOMAIN}/page1'

    await manager.add_request(throttled_url)
    await manager.add_request(free_url)

    # Fetch and handle both.
    req1 = await manager.fetch_next_request()
    assert req1 is not None
    await manager.mark_request_as_handled(req1)

    req2 = await manager.fetch_next_request()
    assert req2 is not None
    await manager.mark_request_as_handled(req2)

    assert await manager.get_handled_count() == 2


async def test_get_total_count_aggregates(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """get_total_count should sum inner and all sub-managers."""
    throttled_url = f'https://{THROTTLED_DOMAIN}/page1'
    free_url = f'https://{NON_THROTTLED_DOMAIN}/page1'

    await manager.add_request(throttled_url)
    await manager.add_request(free_url)

    assert await manager.get_total_count() == 2


async def test_is_empty_aggregates(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """is_empty should return False if any manager that is not in a cooldown has requests."""
    assert await manager.is_empty() is True

    await manager.add_request(f'https://{THROTTLED_DOMAIN}/page1')
    assert await manager.is_empty() is False


async def test_is_finished_aggregates(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """is_finished should return True only when all managers are finished."""
    assert await manager.is_finished() is True

    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)
    assert await manager.is_finished() is False

    request = await manager.fetch_next_request()
    assert request is not None
    await manager.mark_request_as_handled(request)

    assert await manager.is_finished() is True


async def test_drop_clears_all(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """drop should clear inner and all sub-managers."""
    await manager.add_request(f'https://{THROTTLED_DOMAIN}/page1')

    assert THROTTLED_DOMAIN in manager._sub_managers

    await manager.drop()

    assert len(manager._sub_managers) == 0


async def test_purge_clears_requests_and_resets_throttle_state(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """purge should empty inner and sub-managers, reset transient backoff, and preserve domain configuration."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)
    manager.record_domain_delay(url)

    state = manager._domain_states[THROTTLED_DOMAIN]
    assert state.consecutive_429_count == 1
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)

    await manager.purge()

    # Domain configuration is preserved.
    assert THROTTLED_DOMAIN in manager._domain_states
    # Manager is empty.
    assert await manager.is_empty()
    # Transient backoff state is reset.
    assert state.consecutive_429_count == 0
    assert not manager._is_domain_throttled(THROTTLED_DOMAIN)


# ── Utility Tests ──────────────────────────────────────


def test_parse_retry_after_none_value() -> None:
    assert parse_retry_after_header(None) is None


def test_parse_retry_after_empty_string() -> None:
    assert parse_retry_after_header('') is None


def test_parse_retry_after_integer_seconds() -> None:
    result = parse_retry_after_header('120')
    assert result == timedelta(seconds=120)


def test_parse_retry_after_zero_seconds() -> None:
    """A delay of `0` ("retry immediately") is valid and must yield a zero delta, not None."""
    assert parse_retry_after_header('0') == timedelta(0)


def test_parse_retry_after_negative_seconds() -> None:
    """`delay-seconds` is non-negative per RFC 7231; a malformed negative value must be ignored."""
    assert parse_retry_after_header('-5') is None


def test_parse_retry_after_invalid_value() -> None:
    assert parse_retry_after_header('not-a-date-or-number') is None


def test_parse_retry_after_http_date_with_timezone() -> None:
    """A future HTTP-date with explicit timezone should yield a positive delay."""
    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    header = future.strftime('%a, %d %b %Y %H:%M:%S GMT')

    result = parse_retry_after_header(header)

    assert result is not None
    assert timedelta(seconds=60) < result <= timedelta(seconds=121)


def test_parse_retry_after_naive_http_date_treated_as_utc() -> None:
    """A future HTTP-date without timezone info should be treated as UTC, not raise on subtraction."""
    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    # Format without any timezone designator — `parsedate_to_datetime` returns a naive datetime here.
    header = future.strftime('%a, %d %b %Y %H:%M:%S')

    result = parse_retry_after_header(header)

    assert result is not None
    assert timedelta(seconds=60) < result <= timedelta(seconds=121)
