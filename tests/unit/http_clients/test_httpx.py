from __future__ import annotations

import logging
from typing import Any

import httpx2
import pytest

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
        assert client._get_client(None)._mounts == {}
        assert client._get_client(None)._transport is not client_kwargs.get('transport')


def test_silences_httpx_request_logging() -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    httpx_logger = logging.getLogger('httpx2')
    httpx_logger.setLevel(logging.NOTSET)

    HttpxHttpClient()

    assert httpx_logger.level == logging.WARNING
