"""HTTP utility functions for Crawlee."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime


def parse_retry_after_header(value: str | None) -> timedelta | None:
    """Parse the Retry-After HTTP header value.

    The header can contain either a number of seconds or an HTTP-date.
    See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After

    Args:
        value: The raw Retry-After header value.

    Returns:
        A timedelta representing the delay, or None if the header is missing or unparsable.
    """
    if not value:
        return None

    try:
        return timedelta(seconds=int(value))
    except ValueError:
        pass

    try:
        retry_date = parsedate_to_datetime(value)
        delay = retry_date - datetime.now(retry_date.tzinfo or timezone.utc)
        if delay.total_seconds() > 0:
            return delay
    except (ValueError, TypeError):
        pass

    return None
