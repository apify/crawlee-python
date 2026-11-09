from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal
from unittest.mock import AsyncMock, Mock

import pytest
from impit import AsyncClient, Response

from crawlee import HttpHeaders
from crawlee.http_clients import ImpitHttpClient


@pytest.mark.parametrize(
    ('timeout', 'expected_kwargs'),
    [
        pytest.param('default', {}, id='client-default'),
        pytest.param(None, {'timeout': None}, id='no-timeout'),
        pytest.param(timedelta(seconds=5), {'timeout': 5.0}, id='explicit'),
    ],
)
async def test_timeout_passed_to_impit(
    monkeypatch: pytest.MonkeyPatch, timeout: timedelta | Literal['default'] | None, expected_kwargs: dict[str, Any]
) -> None:
    """Test that the timeout reaches `impit` in seconds, as `None`, or not at all for the default of the client."""
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
        timeout=timeout,
    )

    request_kwargs = impit_client.request.call_args.kwargs
    assert {key: value for key, value in request_kwargs.items() if key == 'timeout'} == expected_kwargs
