"""HTTP utility functions for Crawlee."""

from __future__ import annotations

import re
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from logging import getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee.http_clients import HttpResponse

logger = getLogger(__name__)

_MAX_DRAINED_BODY_SIZE = 64 * 1024
"""The largest unread body that `drain_small_body` reads."""

_CHARSET_PATTERN = re.compile(r'(?:^|;)\s*charset\s*=\s*(?:"([^"]*)"|([^;\s]*))', re.IGNORECASE)


def parse_content_type_charset(value: str | None) -> str | None:
    """Get the `charset` parameter of a `Content-Type` header value, if it has one."""
    if not value:
        return None
    match = _CHARSET_PATTERN.search(value)
    if match is None:
        return None
    return (match.group(1) or match.group(2) or '').strip() or None


def parse_retry_after_header(value: str | None) -> timedelta | None:
    """Parse the Retry-After HTTP header value.

    The header can contain either a number of seconds or an HTTP-date.
    See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After

    Args:
        value: The raw Retry-After header value.

    Returns:
        A timedelta representing the delay, or None if the header is missing, unparsable, or not a positive delay.
    """
    if not value:
        return None

    # Numeric form: `delay-seconds`, a non-negative integer per RFC 7231 §7.1.3.
    try:
        seconds = int(value)
    except ValueError:
        pass  # Not an integer, fall through to the HTTP-date form below.
    else:
        if seconds <= 0:
            # A negative delay is malformed and a zero one carries no backoff, so reject both and let the caller
            # apply its own backoff instead.
            logger.debug(f'Retry-After delay-seconds {value!r} is not positive; ignoring.')
            return None
        return timedelta(seconds=seconds)

    # HTTP-date form, e.g. "Wed, 21 Oct 2015 07:28:00 GMT".
    try:
        retry_date = parsedate_to_datetime(value)
        # `parsedate_to_datetime` may return a naive datetime when the input has no timezone info.
        # Treat such values as UTC — HTTP-dates are GMT per RFC 7231.
        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=UTC)

        delay = retry_date - datetime.now(UTC)
        if delay.total_seconds() > 0:
            return delay
        logger.debug(f'Retry-After HTTP-date {value!r} is in the past; ignoring.')
    except (ValueError, TypeError):
        pass

    return None


async def drain_small_body(response: HttpResponse) -> None:
    """Read the body of a response known to be small, so that closing the response keeps its HTTP/1.1 connection.

    `httpx2` and `impit` drop the connection of a response closed before its body arrives.
    """
    content_length = response.headers.get('content-length', '')
    if content_length.isdigit() and int(content_length) <= _MAX_DRAINED_BODY_SIZE:
        # If reading fails, closing the response drops the connection anyway.
        with suppress(Exception):
            await response.read()
