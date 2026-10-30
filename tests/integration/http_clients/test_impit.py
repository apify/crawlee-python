from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

from impit import USE_CLIENT_DEFAULT, AsyncClient, Response

from crawlee import HttpHeaders
from crawlee.http_clients import ImpitHttpClient

if TYPE_CHECKING:
    import pytest


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
    )

    assert impit_client.request.call_args.kwargs['timeout'] is USE_CLIENT_DEFAULT
