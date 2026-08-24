from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.crawlers._playwright._playwright_http_client import PlaywrightHttpClient, browser_page_context


async def test_send_request_zero_timeout_expires_immediately() -> None:
    """A `timedelta(0)` timeout must expire immediately rather than being forwarded as "disable timeout"."""
    page = Mock()
    page.request.fetch = AsyncMock()
    client = PlaywrightHttpClient()

    async with browser_page_context(page):
        with pytest.raises(asyncio.TimeoutError):
            await client.send_request('https://example.com', timeout=timedelta(0))

    page.request.fetch.assert_not_awaited()
