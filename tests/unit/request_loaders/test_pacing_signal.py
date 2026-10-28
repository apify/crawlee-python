from __future__ import annotations

from datetime import timedelta

import pytest

from crawlee.request_loaders import MinIntervalSignal, PacingSignal, RateLimitedSignal


@pytest.mark.parametrize(
    ('signal', 'reason'),
    [
        pytest.param(RateLimitedSignal('https://example.com'), 'rate_limited', id='rate-limited'),
        pytest.param(
            MinIntervalSignal('https://example.com', interval=timedelta(seconds=1), scope='hostname'),
            'min_interval',
            id='min-interval',
        ),
    ],
)
def test_signal_reason(signal: PacingSignal, reason: str) -> None:
    """Test that each signal carries its own reason."""
    assert signal.reason == reason
