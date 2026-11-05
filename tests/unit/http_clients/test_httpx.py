from __future__ import annotations

import logging
import ssl
from typing import Any
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


def test_silences_httpx_request_logging() -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    httpx_logger = logging.getLogger('httpx2')
    httpx_logger.setLevel(logging.NOTSET)

    HttpxHttpClient()

    assert httpx_logger.level == logging.WARNING
