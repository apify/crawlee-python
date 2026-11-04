from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest
from impit import USE_CLIENT_DEFAULT, AsyncClient, Response

from crawlee import HttpHeaders
from crawlee.http_clients import ImpitHttpClient


async def test_no_timeout_uses_client_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a request without a timeout gets the default timeout of the `impit` client, not none at all."""
    impit_client = Mock(spec=AsyncClient)
    impit_client.request = AsyncMock(return_value=Mock(spec=Response))
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_get_client', Mock(return_value=impit_client))

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=None,
        ignore_tls_errors=False,
    )

    assert impit_client.request.call_args.kwargs['timeout'] is USE_CLIENT_DEFAULT


@pytest.mark.parametrize(
    ('client_flag', 'request_flag', 'expected_verify'),
    [
        pytest.param(False, False, True, id='verified'),
        pytest.param(True, False, False, id='client'),
        pytest.param(False, True, False, id='request'),
    ],
)
async def test_ignore_tls_errors_turns_off_verification(
    monkeypatch: pytest.MonkeyPatch,
    *,
    client_flag: bool,
    request_flag: bool,
    expected_verify: bool,
) -> None:
    """Test that the `impit` client skips verification when the client or the request ignores TLS errors."""
    async_client_class = Mock()
    async_client_class.return_value.request = AsyncMock(return_value=Mock(spec=Response))
    monkeypatch.setattr('crawlee.http_clients._impit.AsyncClient', async_client_class)
    client = ImpitHttpClient(ignore_tls_errors=client_flag)

    await client._fetch(
        'https://example.com/',
        method='GET',
        headers=HttpHeaders(),
        payload=None,
        proxy_info=None,
        timeout=None,
        ignore_tls_errors=request_flag,
    )

    assert async_client_class.call_args.kwargs['verify'] is expected_verify
