from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest
from curl_cffi.requests import AsyncSession, Response
from curl_cffi.requests.exceptions import IncompleteRead

from crawlee import HttpHeaders
from crawlee.http_clients import CurlImpersonateHttpClient
from crawlee.http_clients._curl_impersonate import _CurlImpersonateResponse
from crawlee.sessions import Session

if TYPE_CHECKING:
    from yarl import URL


async def test_internal_session_cookie_jar_stays_empty(server_url: URL) -> None:
    """Test that response cookies never accumulate in the jar of the internally cached `curl_cffi` session."""
    session = Session()
    client = CurlImpersonateHttpClient()

    async with client:
        for index in range(5):
            url = server_url.with_path('set_cookies').extend_query(**{f'c{index}': index})
            await client.send_request(str(url), session=session)

        curl_session = client._client_by_proxy_url[None]
        assert list(curl_session._cookies.jar) == []

    # The cookies are stored by `send_request`, so they still reach the crawlee session.
    assert {cookie['name'] for cookie in session.cookies} == {f'c{index}' for index in range(5)}


async def test_read_after_failed_read_raises() -> None:
    """Test that a body whose reading failed halfway can't be read again as an empty body."""
    curl_response = Mock(spec=Response)
    curl_response.acontent.side_effect = [IncompleteRead('Connection dropped'), b'']
    response = _CurlImpersonateResponse(curl_response, deadline=None)

    with pytest.raises(IncompleteRead):
        await response.read()

    with pytest.raises(RuntimeError):
        await response.read()


def mock_curl_session() -> Mock:
    """Build a `curl_cffi` session whose requests return a mock response."""
    curl_session = Mock(spec=AsyncSession)
    curl_session.request = AsyncMock(return_value=Mock(spec=Response))
    return curl_session


async def test_sub_millisecond_timeout_not_rounded_to_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a timeout under 1 ms reaches `curl_cffi` as 1 ms, since `libcurl` reads 0 ms as no timeout."""
    curl_session = mock_curl_session()
    client = CurlImpersonateHttpClient()
    monkeypatch.setattr(client, '_get_client', Mock(return_value=curl_session))

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=timedelta(microseconds=100),
    )

    assert curl_session.request.call_args.kwargs['timeout'] == 0.001


async def test_no_timeout_uses_session_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a request without a timeout gets the default timeout of the `curl_cffi` session, not none at all."""
    curl_session = mock_curl_session()
    client = CurlImpersonateHttpClient()
    monkeypatch.setattr(client, '_get_client', Mock(return_value=curl_session))

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=None,
    )

    assert 'timeout' not in curl_session.request.call_args.kwargs


async def test_timed_out_request_closes_connection() -> None:
    """Test that a request that runs out of time before its response arrives closes the connection."""
    client_closed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b'\r\n\r\n')
        # The server never answers, it only waits for the client to close the connection.
        await reader.read()
        client_closed.set()
        writer.close()

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]

    async with server, CurlImpersonateHttpClient() as client:
        with pytest.raises(TimeoutError):
            await client.send_request(f'http://127.0.0.1:{port}/', timeout=timedelta(seconds=0.2))

        await asyncio.wait_for(client_closed.wait(), timeout=2)


async def test_closing_response_aborts_unread_body() -> None:
    """Test that closing a response with an unread body closes the connection and leaves the body unreadable."""
    client_closed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b'\r\n\r\n')
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n')
        try:
            for _ in range(1000):
                writer.write(b'x' * 1000)
                await writer.drain()
                await asyncio.sleep(0.01)
        except ConnectionError:
            client_closed.set()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]

    async with server, CurlImpersonateHttpClient() as client:
        response = await client.send_request(f'http://127.0.0.1:{port}/')
        await response.aclose()

        await asyncio.wait_for(client_closed.wait(), timeout=2)

        with pytest.raises(RuntimeError):
            await response.read()
