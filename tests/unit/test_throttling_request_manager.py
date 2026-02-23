"""Tests for ThrottlingRequestManager - per-domain delay scheduling.

Tests cover: 429 backoff, robots.txt crawl-delay, domain independence,
exponential backoff, buffer + sleep behavior, and full RequestManager delegation.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawlee._request import Request
from crawlee.request_loaders._throttling_request_manager import ThrottlingRequestManager, _DomainState
from crawlee._utils.http import parse_retry_after_header


# ── Fixtures ──────────────────────────────────────────────


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


def _make_request(url: str) -> Request:
    """Helper to create a Request object."""
    return Request.from_url(url)


# ── Core Throttling Tests ─────────────────────────────────


class TestDomainThrottling:
    """Tests for per-domain rate limiting."""

    @pytest.mark.asyncio
    async def test_non_throttled_passes_through(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """Requests for non-throttled domains should return immediately."""
        request = _make_request('https://example.com/page1')
        mock_inner.fetch_next_request.return_value = request

        result = await manager.fetch_next_request()

        assert result is not None
        assert result.url == 'https://example.com/page1'

    @pytest.mark.asyncio
    async def test_429_triggers_domain_delay(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """After record_domain_delay(), the domain should be throttled."""
        manager.record_domain_delay('https://example.com/page1')

        assert manager._is_domain_throttled('example.com')

    @pytest.mark.asyncio
    async def test_different_domains_independent(self, manager: ThrottlingRequestManager) -> None:
        """Throttling example.com should NOT affect other-site.com."""
        manager.record_domain_delay('https://example.com/page1')

        assert manager._is_domain_throttled('example.com')
        assert not manager._is_domain_throttled('other-site.com')

    @pytest.mark.asyncio
    async def test_exponential_backoff(self, manager: ThrottlingRequestManager) -> None:
        """Consecutive 429s should increase delay exponentially."""
        url = 'https://example.com/page1'

        # First 429: 2s delay.
        manager.record_domain_delay(url)
        state = manager._domain_states['example.com']
        first_until = state.throttled_until

        # Second 429: 4s delay.
        manager.record_domain_delay(url)
        second_until = state.throttled_until

        # The second delay should extend further into the future.
        assert second_until > first_until
        assert state.consecutive_429_count == 2

    @pytest.mark.asyncio
    async def test_max_delay_cap(self, manager: ThrottlingRequestManager) -> None:
        """Backoff should cap at _MAX_DELAY (60s)."""
        url = 'https://example.com/page1'

        # Trigger many 429s to hit the cap.
        for _ in range(20):
            manager.record_domain_delay(url)

        state = manager._domain_states['example.com']
        now = datetime.now(timezone.utc)
        actual_delay = state.throttled_until - now

        # Should never exceed MAX_DELAY + small tolerance.
        assert actual_delay <= manager._MAX_DELAY + timedelta(seconds=1)

    @pytest.mark.asyncio
    async def test_retry_after_header_priority(self, manager: ThrottlingRequestManager) -> None:
        """Explicit Retry-After should override exponential backoff."""
        url = 'https://example.com/page1'

        # Record with explicit 30s Retry-After.
        manager.record_domain_delay(url, retry_after=timedelta(seconds=30))

        state = manager._domain_states['example.com']
        now = datetime.now(timezone.utc)
        actual_delay = state.throttled_until - now

        # Should be approximately 30s (within tolerance).
        assert actual_delay > timedelta(seconds=28)
        assert actual_delay <= timedelta(seconds=31)

    @pytest.mark.asyncio
    async def test_success_resets_backoff(self, manager: ThrottlingRequestManager) -> None:
        """Successful request should reset the consecutive 429 count."""
        url = 'https://example.com/page1'

        manager.record_domain_delay(url)
        manager.record_domain_delay(url)
        assert manager._domain_states['example.com'].consecutive_429_count == 2

        manager.record_success(url)
        assert manager._domain_states['example.com'].consecutive_429_count == 0


# ── Crawl-Delay Integration Tests ─────────────────────────


class TestCrawlDelay:
    """Tests for robots.txt crawl-delay integration (#1396)."""

    @pytest.mark.asyncio
    async def test_crawl_delay_integration(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """set_crawl_delay() should enforce per-domain minimum interval."""
        url = 'https://example.com/page1'
        manager.set_crawl_delay(url, 5)

        state = manager._domain_states['example.com']
        assert state.crawl_delay == timedelta(seconds=5)

    @pytest.mark.asyncio
    async def test_crawl_delay_throttles_after_dispatch(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        """After dispatching a request, crawl-delay should throttle the next one."""
        url = 'https://example.com/page1'
        manager.set_crawl_delay(url, 5)

        # Simulate dispatching (which sets last_request_at).
        manager._mark_domain_dispatched(url)

        # Domain should now be throttled.
        assert manager._is_domain_throttled('example.com')


# ── Sleep-Based Scheduling Tests ────────────────────────


class TestSchedulingBehavior:
    """Tests for the sleep-based scheduling that eliminates busy-wait."""

    @pytest.mark.asyncio
    async def test_mixed_throttled_and_unthrottled(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        """Throttled domain requests should be buffered; unthrottled ones returned."""
        throttled_req = _make_request('https://throttled.com/page1')
        unthrottled_req = _make_request('https://free.com/page1')

        # Throttle one domain.
        manager.record_domain_delay('https://throttled.com/page1')

        # Inner queue returns throttled first, then unthrottled.
        mock_inner.fetch_next_request.side_effect = [throttled_req, unthrottled_req]

        result = await manager.fetch_next_request()

        # Should skip the throttled one and return the unthrottled one.
        assert result is not None
        assert result.url == 'https://free.com/page1'
        # Throttled request should be in the buffer.
        assert len(manager._buffered_requests) == 1

    @pytest.mark.asyncio
    async def test_sleep_instead_of_busy_wait(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        """When all domains are throttled and queue is empty, should sleep (not spin)."""
        throttled_req = _make_request('https://throttled.com/page1')

        # Throttle the domain with a very short delay for test speed.
        manager.record_domain_delay('https://throttled.com/page1', retry_after=timedelta(seconds=0.2))

        # First call returns throttled request, second returns None (queue empty).
        mock_inner.fetch_next_request.side_effect = [throttled_req, None]

        with patch('crawlee.request_loaders._throttling_request_manager.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            # Make sleep a no-op but track that it was called.
            mock_sleep.return_value = None

            # After sleep, the buffered request should be returned.
            # We need the recursive call to find the now-unthrottled buffered request.
            # Reset throttle so the recursive call succeeds.
            async def sleep_side_effect(duration: float) -> None:
                # After sleeping, clear the throttle so the request can be dispatched.
                manager._domain_states['throttled.com'].throttled_until = datetime.now(timezone.utc)

            mock_sleep.side_effect = sleep_side_effect

            result = await manager.fetch_next_request()

            # asyncio.sleep should have been called instead of busy-waiting.
            mock_sleep.assert_called_once()
            assert result is not None
            assert result.url == 'https://throttled.com/page1'


# ── Delegation Tests ────────────────────────────────────


class TestRequestManagerDelegation:
    """Verify all RequestManager methods properly delegate to inner."""

    @pytest.mark.asyncio
    async def test_add_request_delegates(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        request = _make_request('https://example.com')
        await manager.add_request(request)
        mock_inner.add_request.assert_called_once_with(request, forefront=False)

    @pytest.mark.asyncio
    async def test_reclaim_request_delegates(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        request = _make_request('https://example.com')
        await manager.reclaim_request(request)
        mock_inner.reclaim_request.assert_called_once_with(request, forefront=False)

    @pytest.mark.asyncio
    async def test_mark_request_as_handled_delegates(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        request = _make_request('https://example.com')
        await manager.mark_request_as_handled(request)
        mock_inner.mark_request_as_handled.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_get_handled_count_delegates(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        mock_inner.get_handled_count.return_value = 42
        assert await manager.get_handled_count() == 42

    @pytest.mark.asyncio
    async def test_get_total_count_delegates(
        self, manager: ThrottlingRequestManager, mock_inner: AsyncMock
    ) -> None:
        mock_inner.get_total_count.return_value = 100
        assert await manager.get_total_count() == 100

    @pytest.mark.asyncio
    async def test_is_empty_with_buffer(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """is_empty should return False if there are buffered requests."""
        mock_inner.is_empty.return_value = True
        assert await manager.is_empty() is True

        # Add a buffered request.
        manager._buffered_requests.append(_make_request('https://example.com'))
        assert await manager.is_empty() is False

    @pytest.mark.asyncio
    async def test_is_finished_with_buffer(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """is_finished should return False if there are buffered requests."""
        mock_inner.is_finished.return_value = True
        assert await manager.is_finished() is True

        manager._buffered_requests.append(_make_request('https://example.com'))
        assert await manager.is_finished() is False

    @pytest.mark.asyncio
    async def test_drop_clears_buffer(self, manager: ThrottlingRequestManager, mock_inner: AsyncMock) -> None:
        """drop() should clear the buffer and delegate."""
        manager._buffered_requests.append(_make_request('https://example.com'))
        await manager.drop()
        assert len(manager._buffered_requests) == 0
        mock_inner.drop.assert_called_once()


# ── Utility Tests ──────────────────────────────────────


class TestParseRetryAfterHeader:
    """Tests for the extracted parse_retry_after_header utility."""

    def test_none_value(self) -> None:
        assert parse_retry_after_header(None) is None

    def test_empty_string(self) -> None:
        assert parse_retry_after_header('') is None

    def test_integer_seconds(self) -> None:
        result = parse_retry_after_header('120')
        assert result == timedelta(seconds=120)

    def test_invalid_value(self) -> None:
        assert parse_retry_after_header('not-a-date-or-number') is None
