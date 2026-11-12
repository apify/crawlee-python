from __future__ import annotations

import logging
import ssl
from datetime import timedelta
from typing import Any, Literal
from unittest.mock import AsyncMock, Mock

import httpx2
import pytest

from crawlee import HttpHeaders
from crawlee.http_clients import HttpxHttpClient


@pytest.mark.parametrize(
    ('client_kwargs', 'expected_warning'),
    [
        pytest.param({'mounts': {'all://': httpx2.AsyncHTTPTransport()}}, '`mounts` argument', id='mounts'),
        pytest.param({'transport': httpx2.AsyncHTTPTransport()}, '`transport` argument', id='transport'),
    ],
)
async def test_transport_kwargs_do_not_reach_the_client(client_kwargs: dict[str, Any], expected_warning: str) -> None:
    """Test that kwargs mounting a transport of their own are rejected with a warning, so the proxy keeps working."""
    with pytest.warns(UserWarning, match=expected_warning):
        client = HttpxHttpClient(**client_kwargs)

    async with client:
        assert client._get_client(None, ignore_tls_errors=False)._mounts == {}
        assert client._get_client(None, ignore_tls_errors=False)._transport is not client_kwargs.get('transport')


@pytest.mark.parametrize(
    ('client_flag', 'request_flag', 'expected_verify_mode'),
    [
        pytest.param(False, False, ssl.CERT_REQUIRED, id='verified'),
        pytest.param(True, False, ssl.CERT_NONE, id='client'),
        pytest.param(False, True, ssl.CERT_NONE, id='request'),
    ],
)
async def test_ignore_tls_errors_turns_off_verification(
    monkeypatch: pytest.MonkeyPatch,
    *,
    client_flag: bool,
    request_flag: bool,
    expected_verify_mode: ssl.VerifyMode,
) -> None:
    """Test that the `httpx2` transport skips verification when the client or the request ignores TLS errors."""
    transport_class = Mock()
    transport_class.return_value.handle_async_request = AsyncMock(return_value=httpx2.Response(200))
    monkeypatch.setattr(httpx2, 'AsyncHTTPTransport', transport_class)

    client = HttpxHttpClient(ignore_tls_errors=client_flag)

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=None,
        ignore_tls_errors=request_flag,
    )

    assert transport_class.call_args.kwargs['verify'].verify_mode == expected_verify_mode


def test_client_gets_transport_from_create_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the client gets the transport from `_create_transport`, which verifies the certificates."""
    client = HttpxHttpClient()
    create_transport = Mock()
    create_client = Mock()
    monkeypatch.setattr(client, '_create_transport', create_transport)
    monkeypatch.setattr(client, '_create_client', create_client)

    client._get_client(None, ignore_tls_errors=False)

    assert create_transport.call_args.kwargs['verify'].verify_mode == ssl.CERT_REQUIRED
    assert create_client.call_args.kwargs['transport'] is create_transport.return_value


def test_silences_httpx_request_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.NOTSET)

    try:
        HttpxHttpClient()
        assert httpx_logger.level == logging.WARNING
    finally:
        httpx_logger.setLevel(original_level)


def test_keeps_user_set_httpx_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instantiating the client leaves the `httpx2` logger level alone when the application has set one."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.INFO)

    try:
        HttpxHttpClient()
        assert httpx_logger.level == logging.INFO
    finally:
        httpx_logger.setLevel(original_level)


def test_follows_configured_log_level_of_later_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """A client created after the configured log level changes updates the `httpx2` level an earlier client set."""
    monkeypatch.setattr(HttpxHttpClient, '_httpx2_log_level', None)
    httpx_logger = logging.getLogger('httpx2')
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.NOTSET)

    try:
        monkeypatch.setattr('crawlee.http_clients._httpx.get_configured_log_level', lambda: logging.INFO)
        HttpxHttpClient()
        assert httpx_logger.level == logging.WARNING

        monkeypatch.setattr('crawlee.http_clients._httpx.get_configured_log_level', lambda: logging.DEBUG)
        HttpxHttpClient()
        assert httpx_logger.level == logging.DEBUG
    finally:
        httpx_logger.setLevel(original_level)


@pytest.mark.parametrize(
    ('timeout', 'expected'),
    [
        pytest.param('default', 'missing', id='client-default'),
        pytest.param(None, httpx2.Timeout(None), id='no-timeout'),
        pytest.param(timedelta(seconds=5), httpx2.Timeout(5.0), id='explicit'),
    ],
)
async def test_timeout_passed_to_httpx(
    monkeypatch: pytest.MonkeyPatch,
    timeout: timedelta | Literal['default'] | None,
    expected: httpx2.Timeout | Literal['missing'],
) -> None:
    """Test that the timeout reaches `httpx2` as a `Timeout`, or not at all for the default of the client."""
    httpx_client = Mock(spec=httpx2.AsyncClient)
    httpx_client.send = AsyncMock(return_value=Mock(spec=httpx2.Response))
    client = HttpxHttpClient()
    monkeypatch.setattr(client, '_get_client', Mock(return_value=httpx_client))

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=timeout,
        ignore_tls_errors=False,
    )

    assert httpx_client.build_request.call_args.kwargs.get('timeout', 'missing') == expected
