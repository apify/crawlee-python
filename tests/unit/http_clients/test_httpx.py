from __future__ import annotations

import logging
from typing import Any

import httpx2
import pytest

from crawlee.http_clients import HttpxHttpClient
from crawlee.http_clients._httpx import _HttpxTransport, _same_origin


@pytest.mark.parametrize(
    ('url', 'other', 'expected'),
    [
        pytest.param('http://a.com/x', 'http://a.com/x', True, id='same'),
        pytest.param('http://a.com/x', 'https://a.com/x', False, id='different-scheme'),
        pytest.param('http://a.com/x', 'http://b.com/x', False, id='different-host'),
        pytest.param('http://a.com/x', 'http://a.com:8080/x', False, id='different-port'),
        pytest.param('http://a.com:80/x', 'http://a.com/y', True, id='explicit-default-port'),
        pytest.param('http://a.com/x', 'http://a.com/y', True, id='different-path'),
    ],
)
def test_same_origin(url: str, other: str, *, expected: bool) -> None:
    """Test that two URLs share an origin only when their scheme, host and port match."""
    assert _same_origin(httpx2.URL(url), httpx2.URL(other)) is expected


@pytest.mark.parametrize(
    ('client_kwargs', 'expected_warning'),
    [
        pytest.param({'mounts': {'all://': httpx2.AsyncHTTPTransport()}}, '`mounts` argument', id='mounts'),
        pytest.param({'transport': httpx2.AsyncHTTPTransport()}, '`transport` argument', id='transport'),
    ],
)
async def test_transport_kwargs_do_not_reach_the_client(client_kwargs: dict[str, Any], expected_warning: str) -> None:
    """Test that kwargs mounting a transport of their own are rejected with a warning, so the cookies keep working."""
    with pytest.warns(UserWarning, match=expected_warning):
        client = HttpxHttpClient(**client_kwargs)

    async with client:
        assert client._get_client(None)._mounts == {}
        assert isinstance(client._get_client(None)._transport, _HttpxTransport)


def test_silences_httpx_request_logging() -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    httpx_logger = logging.getLogger('httpx2')
    httpx_logger.setLevel(logging.NOTSET)

    HttpxHttpClient()

    assert httpx_logger.level == logging.WARNING
