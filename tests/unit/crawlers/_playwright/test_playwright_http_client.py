from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, Mock, patch

from crawlee.crawlers._playwright._playwright_http_client import PlaywrightHttpClient, browser_page_context
from crawlee.crawlers._playwright._types import PlaywrightHttpResponse


async def test_send_request_converts_timeout_to_milliseconds() -> None:
    playwright_response = Mock()
    expected_response = Mock()
    page = Mock()
    page.request.fetch = AsyncMock(return_value=playwright_response)
    client = PlaywrightHttpClient()

    with patch.object(
        PlaywrightHttpResponse,
        'from_playwright_response',
        new=AsyncMock(return_value=expected_response),
    ) as from_playwright_response:
        async with browser_page_context(page):
            result = await client.send_request('https://example.com', timeout=timedelta(seconds=12))

    assert result is expected_response
    page.request.fetch.assert_awaited_once_with(
        url_or_request='https://example.com',
        method='get',
        headers=None,
        data=None,
        timeout=12_000,
    )
    from_playwright_response.assert_awaited_once_with(playwright_response, protocol='')


async def test_send_request_preserves_zero_timeout() -> None:
    playwright_response = Mock()
    page = Mock()
    page.request.fetch = AsyncMock(return_value=playwright_response)
    client = PlaywrightHttpClient()

    with patch.object(
        PlaywrightHttpResponse,
        'from_playwright_response',
        new=AsyncMock(return_value=Mock()),
    ):
        async with browser_page_context(page):
            await client.send_request('https://example.com', timeout=timedelta(0))

    page.request.fetch.assert_awaited_once_with(
        url_or_request='https://example.com',
        method='get',
        headers=None,
        data=None,
        timeout=0,
    )
