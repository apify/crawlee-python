"""HTTP utility functions for Crawlee."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from logging import getLogger

logger = getLogger(__name__)


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
        seconds = int(value)
        # `delay-seconds` is a non-negative integer per RFC 7231; ignore malformed negative values,
        # consistent with the HTTP-date branch below which also rejects non-positive delays.
        if seconds >= 0:
            return timedelta(seconds=seconds)
    except ValueError:
        pass

    try:
        retry_date = parsedate_to_datetime(value)
        # `parsedate_to_datetime` may return a naive datetime when the input has no timezone info.
        # Treat such values as UTC — HTTP-dates are GMT per RFC 7231.
        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=timezone.utc)

        delay = retry_date - datetime.now(timezone.utc)
        if delay.total_seconds() > 0:
            return delay
        logger.debug(f'Retry-After HTTP-date {value!r} is in the past; ignoring.')
    except (ValueError, TypeError):
        pass

    return None
