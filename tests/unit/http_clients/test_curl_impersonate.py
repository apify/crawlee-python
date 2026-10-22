from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import pytest
from curl_cffi import CurlError
from curl_cffi.requests import AsyncSession, Response
from curl_cffi.requests.exceptions import IncompleteRead

from crawlee import HttpHeaders
from crawlee.http_clients import CurlImpersonateHttpClient
from crawlee.http_clients._curl_impersonate import _CurlImpersonateResponse, _transfers
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
    response = _CurlImpersonateResponse(
        curl_response,
        transfer=asyncio.get_running_loop().create_future(),
        deadline=None,
    )

    with pytest.raises(IncompleteRead):
        await response.read()

    with pytest.raises(RuntimeError):
        await response.read()


def mock_curl_session() -> Mock:
    """Build a `curl_cffi` session whose requests register their transfer, the same as a real request does."""
    curl_session = Mock(spec=AsyncSession)

    async def request(**_kwargs: Any) -> Mock:
        if (transfers := _transfers.get()) is not None:
            transfers.append(asyncio.get_running_loop().create_future())
        return Mock(spec=Response)

    curl_session.request = AsyncMock(side_effect=request)
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


async def test_closing_retried_response_aborts_last_transfer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that closing the response of a retried request aborts the transfer of the attempt that produced it."""
    loop = asyncio.get_running_loop()
    failed_attempt = loop.create_future()
    failed_attempt.set_result(None)
    last_attempt = loop.create_future()

    async def request(**_kwargs: Any) -> Mock:
        if (transfers := _transfers.get()) is not None:
            transfers.extend([failed_attempt, last_attempt])
        return Mock(spec=Response)

    curl_session = Mock(spec=AsyncSession)
    curl_session.request = AsyncMock(side_effect=request)
    client = CurlImpersonateHttpClient()
    monkeypatch.setattr(client, '_get_client', Mock(return_value=curl_session))

    response = await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=None,
    )
    await response.aclose()

    assert last_attempt.done()
    assert isinstance(last_attempt.exception(), CurlError)


async def test_cancelled_request_closes_connection() -> None:
    """Test that a request cancelled before its response arrives closes the connection instead of running on."""
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
            async with asyncio.timeout(0.2):
                await client.send_request(f'http://127.0.0.1:{port}/')

        await asyncio.wait_for(client_closed.wait(), timeout=2)


async def test_closing_response_aborts_unread_body() -> None:
    """Test that closing a response with an unread body closes the connection instead of downloading the rest."""
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
