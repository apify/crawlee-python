"""Tests for ThrottlingRequestManager - per-domain delay scheduling."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, patch

if TYPE_CHECKING:
    from collections.abc import Iterator

import pytest

from crawlee._request import Request
from crawlee._utils.http import parse_retry_after_header
from crawlee.request_loaders._throttling_request_manager import ThrottlingRequestManager

THROTTLED_DOMAIN = 'throttled.com'
NON_THROTTLED_DOMAIN = 'free.com'
TEST_DOMAINS = [THROTTLED_DOMAIN]


@pytest.fixture
def mock_inner() -> AsyncMock:
    """Create a mock RequestManager to wrap."""
    inner = AsyncMock()
    inner.fetch_next_request = AsyncMock(return_value=None)
    inner.add_request = AsyncMock()
    inner.add_requests = AsyncMock()
    inner.reclaim_request = AsyncMock()
    inner.mark_request_as_handled = AsyncMock()
    inner.get_handled_count = AsyncMock(return_value=0)
    inner.get_total_count = AsyncMock(return_value=0)
    inner.is_empty = AsyncMock(return_value=True)
    inner.is_finished = AsyncMock(return_value=True)
    inner.drop = AsyncMock()
    return inner


@pytest.fixture
def manager(mock_inner: AsyncMock) -> ThrottlingRequestManager:
    """Create a ThrottlingRequestManager wrapping the mock with test domains."""
    return ThrottlingRequestManager(mock_inner, domains=TEST_DOMAINS)


@pytest.fixture(autouse=True)
def mock_request_queue_open() -> Iterator[AsyncMock]:
    """Mock RequestQueue.open to avoid hitting real storage during tests."""
    target = 'crawlee.request_loaders._throttling_request_manager.RequestQueue.open'
    with patch(target, new_callable=AsyncMock) as mocked:

        async def mock_open(*_args: Any, **_kwargs: Any) -> AsyncMock:
            sq = AsyncMock()
            sq.fetch_next_request = AsyncMock(return_value=None)
            sq.add_request = AsyncMock()
            sq.add_requests = AsyncMock()
            sq.reclaim_request = AsyncMock()
            sq.mark_request_as_handled = AsyncMock()
            sq.get_handled_count = AsyncMock(return_value=0)
            sq.get_total_count = AsyncMock(return_value=0)
            sq.is_empty = AsyncMock(return_value=True)
            sq.is_finished = AsyncMock(return_value=True)
            sq.drop = AsyncMock()
            return sq

        mocked.side_effect = mock_open
        yield mocked


def _make_request(url: str) -> Request:
    """Helper to create a Request object."""
    return Request.from_url(url)


# ── Request Routing Tests ─────────────────────────────────


@pytest.mark.asyncio
async def test_add_request_routes_listed_domain_to_sub_queue(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
    mock_request_queue_open: AsyncMock,
) -> None:
    """Requests for listed domains should be routed to their sub-queue, not inner."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    await manager.add_request(request)

    mock_request_queue_open.assert_called_once()
    assert THROTTLED_DOMAIN in manager._sub_queues
    sq = manager._sub_queues[THROTTLED_DOMAIN]
    cast('AsyncMock', sq.add_request).assert_called_once_with(request, forefront=False)
    mock_inner.add_request.assert_not_called()


@pytest.mark.asyncio
async def test_add_request_routes_non_listed_domain_to_inner(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """Requests for non-listed domains should go to the inner queue."""
    request = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')
    await manager.add_request(request)

    mock_inner.add_request.assert_called_once_with(request, forefront=False)
    assert NON_THROTTLED_DOMAIN not in manager._sub_queues


@pytest.mark.asyncio
async def test_add_request_with_string_url(
    manager: ThrottlingRequestManager,
    mock_request_queue_open: AsyncMock,
) -> None:
    """add_request should also work when given a plain URL string."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    await manager.add_request(url)

    mock_request_queue_open.assert_called_once()
    sq = manager._sub_queues[THROTTLED_DOMAIN]
    cast('AsyncMock', sq.add_request).assert_called_once_with(url, forefront=False)


@pytest.mark.asyncio
async def test_add_requests_routes_mixed_domains(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """add_requests should split requests by domain and route them correctly."""
    throttled_req = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    free_req = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')

    await manager.add_requests([throttled_req, free_req])

    # Inner gets only the non-listed domain request
    mock_inner.add_requests.assert_called_once()
    inner_call_args = mock_inner.add_requests.call_args
    assert free_req in inner_call_args[0][0]

    # Sub-queue gets the listed domain request
    assert THROTTLED_DOMAIN in manager._sub_queues


# ── Core Throttling Tests ─────────────────────────────────


@pytest.mark.asyncio
async def test_429_triggers_domain_delay(manager: ThrottlingRequestManager) -> None:
    """After record_domain_delay(), the domain should be throttled."""
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1')
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)


@pytest.mark.asyncio
async def test_different_domains_independent(manager: ThrottlingRequestManager) -> None:
    """Throttling one domain should NOT affect other domains."""
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1')
    assert manager._is_domain_throttled(THROTTLED_DOMAIN)
    assert not manager._is_domain_throttled(NON_THROTTLED_DOMAIN)


@pytest.mark.asyncio
async def test_exponential_backoff(manager: ThrottlingRequestManager) -> None:
    """Consecutive 429s should increase delay exponentially."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url)
    state = manager._domain_states[THROTTLED_DOMAIN]
    first_until = state.throttled_until

    manager.record_domain_delay(url)
    second_until = state.throttled_until

    assert second_until > first_until
    assert state.consecutive_429_count == 2


@pytest.mark.asyncio
async def test_max_delay_cap(manager: ThrottlingRequestManager) -> None:
    """Backoff should cap at _MAX_DELAY (60s)."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    for _ in range(20):
        manager.record_domain_delay(url)

    state = manager._domain_states[THROTTLED_DOMAIN]
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay <= manager._MAX_DELAY + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_retry_after_header_priority(manager: ThrottlingRequestManager) -> None:
    """Explicit Retry-After should override exponential backoff."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url, retry_after=timedelta(seconds=30))

    state = manager._domain_states[THROTTLED_DOMAIN]
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay > timedelta(seconds=28)
    assert actual_delay <= timedelta(seconds=31)


@pytest.mark.asyncio
async def test_success_resets_backoff(manager: ThrottlingRequestManager) -> None:
    """Successful request should reset the consecutive 429 count."""
    url = f'https://{THROTTLED_DOMAIN}/page1'

    manager.record_domain_delay(url)
    manager.record_domain_delay(url)
    assert manager._domain_states[THROTTLED_DOMAIN].consecutive_429_count == 2

    manager.record_success(url)
    assert manager._domain_states[THROTTLED_DOMAIN].consecutive_429_count == 0


# ── Crawl-Delay Integration Tests ─────────────────────────


@pytest.mark.asyncio
async def test_crawl_delay_integration(manager: ThrottlingRequestManager) -> None:
    """set_crawl_delay() should record the delay for the domain."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    manager.set_crawl_delay(url, 5)

    state = manager._domain_states[THROTTLED_DOMAIN]
    assert state.crawl_delay == timedelta(seconds=5)


@pytest.mark.asyncio
async def test_crawl_delay_throttles_after_dispatch(manager: ThrottlingRequestManager) -> None:
    """After dispatching a request, crawl-delay should throttle the next one."""
    url = f'https://{THROTTLED_DOMAIN}/page1'
    manager.set_crawl_delay(url, 5)

    manager._mark_domain_dispatched(url)

    assert manager._is_domain_throttled(THROTTLED_DOMAIN)


# ── Fetch Scheduling Tests ────────────────────────────


@pytest.mark.asyncio
async def test_fetch_from_unthrottled_sub_queue(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """fetch_next_request should return from an unthrottled sub-queue."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')

    sq = AsyncMock()
    sq.fetch_next_request = AsyncMock(return_value=request)
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == f'https://{THROTTLED_DOMAIN}/page1'
    mock_inner.fetch_next_request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_falls_back_to_inner(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """When sub-queues are empty, should return from inner queue."""
    request = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')
    mock_inner.fetch_next_request.return_value = request

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == f'https://{NON_THROTTLED_DOMAIN}/page1'


@pytest.mark.asyncio
async def test_fetch_skips_throttled_sub_queue(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """Should skip throttled sub-queues and fall through to inner."""
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1')

    sq = AsyncMock()
    sq.fetch_next_request = AsyncMock(return_value=_make_request(f'https://{THROTTLED_DOMAIN}/page1'))
    sq.is_empty = AsyncMock(return_value=False)
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    inner_req = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')
    mock_inner.fetch_next_request.return_value = inner_req

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == f'https://{NON_THROTTLED_DOMAIN}/page1'


@pytest.mark.asyncio
async def test_sleep_when_all_throttled(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """When all domains are throttled and inner is empty, should sleep and retry."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    manager.record_domain_delay(f'https://{THROTTLED_DOMAIN}/page1', retry_after=timedelta(seconds=0.2))

    sq = AsyncMock()
    sq.is_empty = AsyncMock(return_value=False)
    sq.fetch_next_request = AsyncMock(return_value=request)
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    mock_inner.fetch_next_request.return_value = None

    target = 'crawlee.request_loaders._throttling_request_manager.asyncio.sleep'
    with patch(target, new_callable=AsyncMock) as mock_sleep:

        async def sleep_side_effect(*_args: Any, **_kwargs: Any) -> None:
            manager._domain_states[THROTTLED_DOMAIN].throttled_until = datetime.now(timezone.utc)

        mock_sleep.side_effect = sleep_side_effect

        result = await manager.fetch_next_request()

        mock_sleep.assert_called_once()
        assert result is not None
        assert result.url == f'https://{THROTTLED_DOMAIN}/page1'


# ── Delegation Tests ────────────────────────────────────


@pytest.mark.asyncio
async def test_reclaim_request_routes_to_sub_queue(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """reclaim_request should route to sub-queue for listed domains."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    sq = AsyncMock()
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    await manager.reclaim_request(request)

    sq.reclaim_request.assert_called_once_with(request, forefront=False)
    mock_inner.reclaim_request.assert_not_called()


@pytest.mark.asyncio
async def test_reclaim_request_routes_to_inner(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """reclaim_request should route to inner for non-listed domains."""
    request = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')

    await manager.reclaim_request(request)

    mock_inner.reclaim_request.assert_called_once_with(request, forefront=False)


@pytest.mark.asyncio
async def test_mark_request_as_handled_routes_to_sub_queue(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """mark_request_as_handled should route to sub-queue for listed domains."""
    request = _make_request(f'https://{THROTTLED_DOMAIN}/page1')
    sq = AsyncMock()
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    await manager.mark_request_as_handled(request)

    sq.mark_request_as_handled.assert_called_once_with(request)
    mock_inner.mark_request_as_handled.assert_not_called()


@pytest.mark.asyncio
async def test_mark_request_as_handled_routes_to_inner(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
) -> None:
    """mark_request_as_handled should route to inner for non-listed domains."""
    request = _make_request(f'https://{NON_THROTTLED_DOMAIN}/page1')

    await manager.mark_request_as_handled(request)

    mock_inner.mark_request_as_handled.assert_called_once_with(request)


@pytest.mark.asyncio
async def test_get_handled_count_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """get_handled_count should sum inner and all sub-queues."""
    mock_inner.get_handled_count.return_value = 42

    sq = AsyncMock()
    sq.get_handled_count.return_value = 10
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    assert await manager.get_handled_count() == 52


@pytest.mark.asyncio
async def test_get_total_count_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """get_total_count should sum inner and all sub-queues."""
    mock_inner.get_total_count.return_value = 100

    sq = AsyncMock()
    sq.get_total_count.return_value = 20
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    assert await manager.get_total_count() == 120


@pytest.mark.asyncio
async def test_is_empty_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """is_empty should return False if any queue has requests."""
    mock_inner.is_empty.return_value = True
    assert await manager.is_empty() is True

    sq = AsyncMock()
    sq.is_empty.return_value = False
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    assert await manager.is_empty() is False


@pytest.mark.asyncio
async def test_is_finished_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """is_finished should return False if any queue is not finished."""
    mock_inner.is_finished.return_value = True
    assert await manager.is_finished() is True

    sq = AsyncMock()
    sq.is_finished.return_value = False
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    assert await manager.is_finished() is False


@pytest.mark.asyncio
async def test_drop_clears_all(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """drop should clear inner, all sub-queues, and internal state."""
    sq = AsyncMock()
    manager._sub_queues[THROTTLED_DOMAIN] = sq

    await manager.drop()

    mock_inner.drop.assert_called_once()
    sq.drop.assert_called_once()
    assert len(manager._sub_queues) == 0


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
