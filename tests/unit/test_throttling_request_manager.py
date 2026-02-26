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
    """Create a ThrottlingRequestManager wrapping the mock."""
    return ThrottlingRequestManager(mock_inner)


@pytest.fixture(autouse=True)
def mock_request_queue_open() -> Iterator[AsyncMock]:
    """Mock RequestQueue.open to avoid hitting real storage during tests."""
    target = 'crawlee.request_loaders._throttling_request_manager.RequestQueue.open'
    with patch(target, new_callable=AsyncMock) as mocked:

        async def mock_open(*_args: Any, **_kwargs: Any) -> AsyncMock:
            sq = AsyncMock()
            sq.fetch_next_request = AsyncMock(return_value=None)
            sq.add_request = AsyncMock()
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


# ── Core Throttling Tests ─────────────────────────────────


@pytest.mark.asyncio
async def test_non_throttled_passes_through(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """Requests for non-throttled domains should return immediately."""
    request = _make_request('https://example.com/page1')
    mock_inner.fetch_next_request.return_value = request

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == 'https://example.com/page1'


@pytest.mark.asyncio
async def test_429_triggers_domain_delay(manager: ThrottlingRequestManager) -> None:
    """After record_domain_delay(), the domain should be throttled."""
    manager.record_domain_delay('https://example.com/page1')

    assert manager._is_domain_throttled('example.com')


@pytest.mark.asyncio
async def test_different_domains_independent(manager: ThrottlingRequestManager) -> None:
    """Throttling example.com should NOT affect other-site.com."""
    manager.record_domain_delay('https://example.com/page1')

    assert manager._is_domain_throttled('example.com')
    assert not manager._is_domain_throttled('other-site.com')


@pytest.mark.asyncio
async def test_exponential_backoff(manager: ThrottlingRequestManager) -> None:
    """Consecutive 429s should increase delay exponentially."""
    url = 'https://example.com/page1'

    manager.record_domain_delay(url)
    state = manager._domain_states['example.com']
    first_until = state.throttled_until

    manager.record_domain_delay(url)
    second_until = state.throttled_until

    assert second_until > first_until
    assert state.consecutive_429_count == 2


@pytest.mark.asyncio
async def test_max_delay_cap(manager: ThrottlingRequestManager) -> None:
    """Backoff should cap at _MAX_DELAY (60s)."""
    url = 'https://example.com/page1'

    for _ in range(20):
        manager.record_domain_delay(url)

    state = manager._domain_states['example.com']
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay <= manager._MAX_DELAY + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_retry_after_header_priority(manager: ThrottlingRequestManager) -> None:
    """Explicit Retry-After should override exponential backoff."""
    url = 'https://example.com/page1'

    manager.record_domain_delay(url, retry_after=timedelta(seconds=30))

    state = manager._domain_states['example.com']
    now = datetime.now(timezone.utc)
    actual_delay = state.throttled_until - now

    assert actual_delay > timedelta(seconds=28)
    assert actual_delay <= timedelta(seconds=31)


@pytest.mark.asyncio
async def test_success_resets_backoff(manager: ThrottlingRequestManager) -> None:
    """Successful request should reset the consecutive 429 count."""
    url = 'https://example.com/page1'

    manager.record_domain_delay(url)
    manager.record_domain_delay(url)
    assert manager._domain_states['example.com'].consecutive_429_count == 2

    manager.record_success(url)
    assert manager._domain_states['example.com'].consecutive_429_count == 0


# ── Crawl-Delay Integration Tests ─────────────────────────


@pytest.mark.asyncio
async def test_crawl_delay_integration(manager: ThrottlingRequestManager) -> None:
    """set_crawl_delay() should enforce per-domain minimum interval."""
    url = 'https://example.com/page1'
    manager.set_crawl_delay(url, 5)

    state = manager._domain_states['example.com']
    assert state.crawl_delay == timedelta(seconds=5)


@pytest.mark.asyncio
async def test_crawl_delay_throttles_after_dispatch(manager: ThrottlingRequestManager) -> None:
    """After dispatching a request, crawl-delay should throttle the next one."""
    url = 'https://example.com/page1'
    manager.set_crawl_delay(url, 5)

    manager._mark_domain_dispatched(url)

    assert manager._is_domain_throttled('example.com')


# ── Sleep-Based Scheduling Tests ────────────────────────


@pytest.mark.asyncio
async def test_mixed_throttled_and_unthrottled(
    manager: ThrottlingRequestManager,
    mock_inner: AsyncMock,
    mock_request_queue_open: AsyncMock,
) -> None:
    """Throttled domain requests should be moved to sub-queues; unthrottled ones returned."""
    throttled_req = _make_request('https://throttled.com/page1')
    unthrottled_req = _make_request('https://free.com/page1')

    manager.record_domain_delay('https://throttled.com/page1')

    # inner returns throttled, then unthrottled
    mock_inner.fetch_next_request.side_effect = [throttled_req, unthrottled_req]

    result = await manager.fetch_next_request()

    assert result is not None
    assert result.url == 'https://free.com/page1'

    # Verify throttled request was moved to sub-queue
    mock_request_queue_open.assert_called_once()
    assert 'throttled.com' in manager._sub_queues

    sq = manager._sub_queues['throttled.com']
    cast('AsyncMock', sq.add_request).assert_called_once_with(throttled_req)


@pytest.mark.asyncio
async def test_sleep_instead_of_busy_wait(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    """When all domains are throttled and queue is empty, should sleep (not spin)."""
    throttled_req = _make_request('https://throttled.com/page1')

    manager.record_domain_delay('https://throttled.com/page1', retry_after=timedelta(seconds=0.2))

    # inner queue returns the request first time, then None
    mock_inner.fetch_next_request.side_effect = [throttled_req, None]

    target = 'crawlee.request_loaders._throttling_request_manager.asyncio.sleep'
    with patch(target, new_callable=AsyncMock) as mock_sleep:
        # Instead of actually sleeping, we simulate the time passing by unthrottling the domain
        async def sleep_side_effect(*_args: Any, **_kwargs: Any) -> None:
            # Clear throttle so recursive call succeeds
            manager._domain_states['throttled.com'].throttled_until = datetime.now(timezone.utc)
            # Setup the sub-queue to return the request now
            sq = manager._sub_queues['throttled.com']
            cast('AsyncMock', sq.fetch_next_request).side_effect = [throttled_req, None]
            # Must return False then True so loop proceeds
            cast('AsyncMock', sq.is_empty).side_effect = [False, True]

        mock_sleep.side_effect = sleep_side_effect

        # When request is moved to sub-queue, we must ensure it isn't "empty" so it triggers sleep
        async def mock_add_request(*_args: Any, **_kwargs: Any) -> None:
            sq = manager._sub_queues['throttled.com']
            cast('AsyncMock', sq.is_empty).return_value = False

        manager._sub_queues = {'throttled.com': AsyncMock()}
        manager._sub_queues['throttled.com'].add_request.side_effect = mock_add_request
        manager._sub_queues['throttled.com'].is_empty.return_value = True

        result = await manager.fetch_next_request()

        mock_sleep.assert_called_once()
        assert result is not None
        assert result.url == 'https://throttled.com/page1'


# ── Delegation Tests ────────────────────────────────────


@pytest.mark.asyncio
async def test_add_request_delegates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    request = _make_request('https://example.com')
    await manager.add_request(request)
    mock_inner.add_request.assert_called_once_with(request, forefront=False)


@pytest.mark.asyncio
async def test_reclaim_request_delegates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    request = _make_request('https://example.com')
    await manager.reclaim_request(request)
    mock_inner.reclaim_request.assert_called_once_with(request, forefront=False)


@pytest.mark.asyncio
async def test_reclaim_request_delegates_to_sub_queue(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    request = _make_request('https://example.com')

    # Setup state manually assuming it was fetched from a sub-queue
    sq = AsyncMock()
    manager._sub_queues['example.com'] = sq
    manager._dispatched_origins[request.unique_key] = 'example.com'

    await manager.reclaim_request(request)

    sq.reclaim_request.assert_called_once_with(request, forefront=False)
    mock_inner.reclaim_request.assert_not_called()


@pytest.mark.asyncio
async def test_mark_request_as_handled_delegates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    request = _make_request('https://example.com')
    await manager.mark_request_as_handled(request)
    mock_inner.mark_request_as_handled.assert_called_once_with(request)


@pytest.mark.asyncio
async def test_get_handled_count_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    mock_inner.get_handled_count.return_value = 42

    sq = AsyncMock()
    sq.get_handled_count.return_value = 10
    manager._sub_queues['example.com'] = sq
    manager._transferred_requests_count = 5

    assert await manager.get_handled_count() == 47


@pytest.mark.asyncio
async def test_get_total_count_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    mock_inner.get_total_count.return_value = 100

    sq = AsyncMock()
    sq.get_total_count.return_value = 20
    manager._sub_queues['example.com'] = sq
    manager._transferred_requests_count = 10

    assert await manager.get_total_count() == 110


@pytest.mark.asyncio
async def test_is_empty_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    mock_inner.is_empty.return_value = True
    assert await manager.is_empty() is True

    sq = AsyncMock()
    sq.is_empty.return_value = False
    manager._sub_queues['example.com'] = sq

    assert await manager.is_empty() is False


@pytest.mark.asyncio
async def test_is_finished_aggregates(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    mock_inner.is_finished.return_value = True
    assert await manager.is_finished() is True

    sq = AsyncMock()
    sq.is_finished.return_value = False
    manager._sub_queues['example.com'] = sq

    assert await manager.is_finished() is False


@pytest.mark.asyncio
async def test_drop_clears_all(manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
    request = _make_request('https://example.com')
    sq = AsyncMock()
    manager._sub_queues['example.com'] = sq
    manager._dispatched_origins[request.unique_key] = 'inner'

    await manager.drop()

    mock_inner.drop.assert_called_once()
    sq.drop.assert_called_once()
    assert len(manager._sub_queues) == 0
    assert len(manager._dispatched_origins) == 0


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
