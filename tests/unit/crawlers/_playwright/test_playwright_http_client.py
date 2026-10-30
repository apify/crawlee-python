from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest
from playwright.async_api import APIResponse

from crawlee.crawlers._playwright._playwright_http_client import PlaywrightHttpClient, browser_page_context
from crawlee.crawlers._playwright._types import PlaywrightHttpResponse
from crawlee.http_clients import HttpResponse
from crawlee.sessions import CookieParam, SessionCookies


def mock_page() -> Mock:
    """Build a browser page whose context has no cookies."""
    page = Mock()
    page.context.cookies = AsyncMock(return_value=[])
    page.request.fetch = AsyncMock()
    return page


def mock_response(status_code: int, url: str, *, location: str | None = None) -> Mock:
    """Build a browser response with the given status code and an optional `Location` header."""
    headers = {'location': location} if location else {}
    return Mock(spec=HttpResponse, status_code=status_code, headers=headers, url=url, headers_set_cookie=[])


async def test_send_request_converts_timeout_to_milliseconds() -> None:
    playwright_response = Mock()
    expected_response = mock_response(200, 'https://example.com/')
    page = mock_page()
    page.request.fetch.return_value = playwright_response
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
        timeout=pytest.approx(12_000, abs=100),
        max_redirects=0,
    )
    from_playwright_response.assert_awaited_once_with(playwright_response, protocol='')


async def test_send_request_follows_redirects_hop_by_hop() -> None:
    """Test that `send_request` follows redirects itself and the browser sends every hop without following them."""
    redirect = mock_response(302, 'https://example.com/start', location='/final')
    final = mock_response(200, 'https://example.com/final')
    page = mock_page()
    client = PlaywrightHttpClient()

    with patch.object(PlaywrightHttpResponse, 'from_playwright_response', new=AsyncMock(side_effect=[redirect, final])):
        async with browser_page_context(page):
            result = await client.send_request('https://example.com/start')

    assert result is final
    assert [call.kwargs['url_or_request'] for call in page.request.fetch.await_args_list] == [
        'https://example.com/start',
        'https://example.com/final',
    ]
    assert all(call.kwargs['max_redirects'] == 0 for call in page.request.fetch.await_args_list)


async def test_send_request_sends_cookies_argument_instead_of_context_cookies() -> None:
    """Test that the cookies passed in `cookies` are sent instead of the cookies of the browser context."""
    page = mock_page()
    cookies = SessionCookies([CookieParam(name='from_argument', value='1', domain='example.com')])
    client = PlaywrightHttpClient()

    with patch.object(
        PlaywrightHttpResponse,
        'from_playwright_response',
        new=AsyncMock(return_value=mock_response(200, 'https://example.com/')),
    ):
        async with browser_page_context(page):
            await client.send_request('https://example.com/', cookies=cookies)

    page.context.cookies.assert_not_awaited()
    assert page.request.fetch.call_args.kwargs['headers'] == {'cookie': 'from_argument=1'}


async def test_cookies_argument_keeps_context_cookies_out() -> None:
    """Test that the empty `Cookie` header keeping the browser context cookies out is sent only with `cookies`."""
    page = mock_page()
    client = PlaywrightHttpClient()

    with patch.object(
        PlaywrightHttpResponse,
        'from_playwright_response',
        new=AsyncMock(return_value=mock_response(200, 'https://example.com/')),
    ):
        async with browser_page_context(page):
            await client.send_request('https://example.com/', cookies=SessionCookies())
            await client.send_request('https://example.com/')

    assert [call.kwargs['headers'] for call in page.request.fetch.await_args_list] == [{'cookie': ''}, None]


async def test_cookies_argument_keeps_context_cookies_out_cross_origin() -> None:
    """Test that the browser context adds no cookies of its own after a redirect to another origin."""
    redirect = mock_response(302, 'https://example.com/start', location='https://other.com/final')
    final = mock_response(200, 'https://other.com/final')
    page = mock_page()
    cookies = SessionCookies([CookieParam(name='from_argument', value='1', domain='example.com')])
    client = PlaywrightHttpClient()

    with patch.object(PlaywrightHttpResponse, 'from_playwright_response', new=AsyncMock(side_effect=[redirect, final])):
        async with browser_page_context(page):
            await client.send_request('https://example.com/start', cookies=cookies)

    assert [call.kwargs['headers'] for call in page.request.fetch.await_args_list] == [
        {'cookie': 'from_argument=1'},
        {'cookie': ''},
    ]


async def test_send_request_zero_timeout_expires_immediately() -> None:
    """A `timedelta(0)` timeout must expire immediately rather than being forwarded as "disable timeout"."""
    page = mock_page()
    client = PlaywrightHttpClient()

    async with browser_page_context(page):
        with pytest.raises(TimeoutError):
            await client.send_request('https://example.com', timeout=timedelta(0))

    page.request.fetch.assert_not_awaited()


async def test_response_keeps_url_and_every_set_cookie_header() -> None:
    """Test that a response of the browser keeps its URL and lists every `Set-Cookie` header."""
    api_response = Mock(spec=APIResponse)
    api_response.url = 'https://example.com/final'
    api_response.status = 200
    api_response.headers = {'set-cookie': 'a=1\nb=2', 'content-type': 'text/html'}
    api_response.headers_array = [
        {'name': 'Set-Cookie', 'value': 'a=1'},
        {'name': 'set-cookie', 'value': 'b=2'},
        {'name': 'Content-Type', 'value': 'text/html'},
    ]
    api_response.body.return_value = b''

    response = await PlaywrightHttpResponse.from_playwright_response(api_response, protocol='')

    assert response.url == 'https://example.com/final'
    assert response.headers_set_cookie == ['a=1', 'b=2']
