from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from crawlee._request_throttler import RequestThrottler


class TestRequestThrottler:
    """Tests for the RequestThrottler per-domain rate limit tracker."""

    def test_not_throttled_by_default(self) -> None:
        """Requests should not be throttled when no 429 has been recorded."""
        throttler = RequestThrottler()
        assert not throttler.is_throttled('https://example.com/page1')
        assert throttler.get_delay('https://example.com/page1') == timedelta(0)

    def test_throttled_after_rate_limit(self) -> None:
        """A domain should be throttled after a 429 is recorded."""
        throttler = RequestThrottler()
        throttler.record_rate_limit('https://example.com/page1')

        assert throttler.is_throttled('https://example.com/page2')
        assert throttler.get_delay('https://example.com/page2') > timedelta(0)

    def test_different_domains_independent(self) -> None:
        """A 429 on domain A should not affect domain B."""
        throttler = RequestThrottler()
        throttler.record_rate_limit('https://example.com/page1')

        # example.com should be throttled
        assert throttler.is_throttled('https://example.com/other')

        # other-site.com should NOT be throttled
        assert not throttler.is_throttled('https://other-site.com/page1')
        assert throttler.get_delay('https://other-site.com/page1') == timedelta(0)

    def test_exponential_backoff(self) -> None:
        """Consecutive 429s should increase delay exponentially."""
        throttler = RequestThrottler()

        # First 429: delay = 2s (BASE_DELAY * 2^0)
        throttler.record_rate_limit('https://example.com/a')
        delay_1 = throttler.get_delay('https://example.com/a')

        # Second 429: delay = 4s (BASE_DELAY * 2^1)
        throttler.record_rate_limit('https://example.com/b')
        delay_2 = throttler.get_delay('https://example.com/b')

        # Third 429: delay = 8s (BASE_DELAY * 2^2)
        throttler.record_rate_limit('https://example.com/c')
        delay_3 = throttler.get_delay('https://example.com/c')

        # Each subsequent delay should be roughly double the previous
        assert delay_2 > delay_1
        assert delay_3 > delay_2

    def test_max_delay_cap(self) -> None:
        """Delay should be capped at MAX_DELAY even with many consecutive 429s."""
        throttler = RequestThrottler()

        # Record many 429s to exceed MAX_DELAY
        for _ in range(20):
            throttler.record_rate_limit('https://example.com/page')

        delay = throttler.get_delay('https://example.com/page')
        assert delay <= RequestThrottler._MAX_DELAY

    def test_success_resets_backoff(self) -> None:
        """A successful request should reset the consecutive 429 count."""
        throttler = RequestThrottler()

        # Record multiple 429s
        throttler.record_rate_limit('https://example.com/a')
        throttler.record_rate_limit('https://example.com/b')
        throttler.record_rate_limit('https://example.com/c')

        # Record a success
        throttler.record_success('https://example.com/page')

        # The internal state should show 0 consecutive 429s
        state = throttler._domain_states.get('example.com')
        assert state is not None
        assert state.consecutive_429_count == 0

    def test_retry_after_takes_priority(self) -> None:
        """Retry-After value should take priority over exponential backoff."""
        throttler = RequestThrottler()

        # Record 429 with a specific Retry-After of 30 seconds
        throttler.record_rate_limit('https://example.com/page', retry_after=timedelta(seconds=30))

        delay = throttler.get_delay('https://example.com/page')
        # Delay should be close to 30s (minus time elapsed since recording)
        assert delay > timedelta(seconds=29)
        assert delay <= timedelta(seconds=30)

    def test_throttle_expires_after_delay(self) -> None:
        """A domain should no longer be throttled after the delay expires."""
        throttler = RequestThrottler()

        # Record a 429 and manually set next_allowed_at to the past
        throttler.record_rate_limit('https://example.com/page')
        state = throttler._domain_states['example.com']
        state.next_allowed_at = datetime.now(timezone.utc) - timedelta(seconds=1)

        assert not throttler.is_throttled('https://example.com/page')
        assert throttler.get_delay('https://example.com/page') == timedelta(0)

    def test_empty_url_handling(self) -> None:
        """Empty or invalid URLs should not cause errors."""
        throttler = RequestThrottler()

        # These should not raise
        throttler.record_rate_limit('')
        throttler.record_success('')
        assert not throttler.is_throttled('')


class TestParseRetryAfterHeader:
    """Tests for BasicCrawler._parse_retry_after_header."""

    def test_none_value(self) -> None:
        """None input returns None."""
        from crawlee.crawlers._basic._basic_crawler import BasicCrawler

        assert BasicCrawler._parse_retry_after_header(None) is None

    def test_empty_string(self) -> None:
        """Empty string returns None."""
        from crawlee.crawlers._basic._basic_crawler import BasicCrawler

        assert BasicCrawler._parse_retry_after_header('') is None

    def test_integer_seconds(self) -> None:
        """Integer value should be parsed as seconds."""
        from crawlee.crawlers._basic._basic_crawler import BasicCrawler

        result = BasicCrawler._parse_retry_after_header('120')
        assert result == timedelta(seconds=120)

    def test_invalid_value(self) -> None:
        """Invalid values should return None."""
        from crawlee.crawlers._basic._basic_crawler import BasicCrawler

        assert BasicCrawler._parse_retry_after_header('not-a-number') is None
