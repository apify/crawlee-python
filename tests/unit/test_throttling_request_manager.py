"""Tests for ThrottlingRequestManager - per-domain delay scheduling."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from crawlee._request import Request
from crawlee._service_locator import ServiceLocator
from crawlee._utils.http import parse_retry_after_header
from crawlee.request_loaders._throttling_request_manager import ThrottlingRequestManager
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

THROTTLED_DOMAIN = 'throttled.com'
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
    """After record_domain_delay(), the domain should be throttled."""
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1')
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)


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

    manager._mark_domain_dispatched(url)

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


async def test_sleep_when_all_throttled(manager: ThrottlingRequestManager[RequestQueue]) -> None:
    """When all domains are throttled and inner is empty, should wait and retry."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    manager.record_domain_delay(url, retry_after=timedelta(seconds=10))

    target = (
        'crawlee.request_loaders._throttling_request_manager.ThrottlingRequestManager._wait_for_new_work_or_timeout'
    )
    with patch(target, new_callable=AsyncMock) as mock_wait:

        async def wait_side_effect(*_args: Any, **_kwargs: Any) -> None:
            # Set throttled_until firmly in the past so the next iteration reliably unblocks the domain
            # regardless of clock resolution or scheduling jitter on slow CI runners.
            manager._domain_states[THROTTLED_DOMAIN].throttled_until = datetime.now(timezone.utc) - timedelta(seconds=1)

        mock_wait.side_effect = wait_side_effect

        result = await manager.fetch_next_request()

        mock_wait.assert_called()
        assert result is not None
        assert result.url == url


async def test_fetch_wakes_when_request_added_during_throttle_wait(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """When all sub-managers are throttled and inner is empty, fetch should wake up immediately
    when a new request is added rather than blocking until the throttle expires."""
    # Throttle the only configured domain for a long time so a naive sleep would block here.
    throttled_url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(throttled_url)
    manager.record_domain_delay(throttled_url, retry_after=timedelta(seconds=60))

    free_url = f'https://{NON_THROTTLED_DOMAIN}/page1'

    # Wrap the wait helper so we can synchronize with the moment fetch enters the wait state.
    wait_entered = asyncio.Event()
    original_wait = manager._wait_for_new_work_or_timeout

    async def signaling_wait(timeout: float) -> None:
        wait_entered.set()
        await original_wait(timeout)

    manager._wait_for_new_work_or_timeout = signaling_wait  # ty: ignore[invalid-assignment]

    fetch_task = asyncio.create_task(manager.fetch_next_request())

    # Wait until fetch is suspended inside the wait, then add fresh non-throttled work.
    await wait_entered.wait()
    await manager.add_request(free_url)

    # If the wake-up signal works, fetch returns the freshly-added request well within the
    # 2s wait_for budget; otherwise it would still be blocked on the 60s throttle.
    result = await asyncio.wait_for(fetch_task, timeout=2.0)

    assert result is not None
    assert result.url == free_url


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
    """is_empty should return False if any manager has requests."""
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


async def test_recreate_purged(
    manager: ThrottlingRequestManager[RequestQueue],
) -> None:
    """recreate_purged should return a fresh manager with the same domain configuration."""
    await manager.add_request(f'https://{THROTTLED_DOMAIN}/page1')

    new_manager = await manager.recreate_purged()

    # Same domains should be configured.
    assert THROTTLED_DOMAIN in new_manager._domain_states
    # But managers should be empty.
    assert await new_manager.is_empty()


# ── Utility Tests ──────────────────────────────────────


def test_parse_retry_after_none_value() -> None:
    assert parse_retry_after_header(None) is None


def test_parse_retry_after_empty_string() -> None:
    assert parse_retry_after_header('') is None


def test_parse_retry_after_integer_seconds() -> None:
    result = parse_retry_after_header('120')
    assert result == timedelta(seconds=120)


def test_parse_retry_after_invalid_value() -> None:
    assert parse_retry_after_header('not-a-date-or-number') is None
