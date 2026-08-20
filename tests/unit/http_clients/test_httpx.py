from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock

import httpx
import pytest

from crawlee import HttpHeaders
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT_LANGUAGE
from crawlee.http_clients import HttpxHttpClient
from crawlee.http_clients._httpx import _same_origin

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients import HttpResponse


async def read_json(response: HttpResponse) -> dict:
    return json.loads((await response.read()).decode())


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
    assert _same_origin(httpx.URL(url), httpx.URL(other)) is expected


def test_proxy_kwarg_does_not_reach_the_client() -> None:
    """Test that a `proxy` kwarg cannot mount a transport that would bypass the cookie handling."""
    client = HttpxHttpClient(proxy='http://user:password@127.0.0.1:8888')

    assert client._get_client(None)._mounts == {}


def test_silences_httpx_request_logging() -> None:
    """Instantiating the client lowers the noisy per-request `httpx` INFO logs to WARNING by default."""
    httpx_logger = logging.getLogger('httpx')
    httpx_logger.setLevel(logging.NOTSET)

    HttpxHttpClient()

    assert httpx_logger.level == logging.WARNING


async def test_common_headers_and_user_agent(server_url: URL, header_network: dict) -> None:
    """Test that the relevant headers use header values from header generator instead of default Httpx headers.

    Httpx uses own headers by default which is not desired as it could increase blocking chances.
    """
    client = HttpxHttpClient()

    response = await client.send_request(str(server_url / 'headers'))
    response_headers = json.loads((await response.read()).decode())

    assert 'accept' in response_headers
    assert response_headers['accept'] in get_available_header_values(header_network, {'Accept', 'accept'})

    assert 'accept-language' in response_headers
    assert response_headers['accept-language'] == COMMON_ACCEPT_LANGUAGE

    # By default, HTTPX uses its own User-Agent, which should be replaced by the one from the header generator.
    assert 'user-agent' in response_headers
    assert 'python-httpx' not in response_headers['user-agent']
    assert response_headers['user-agent'] in get_available_header_values(header_network, {'User-Agent', 'user-agent'})


async def test_headers_come_from_one_sample(server_url: URL) -> None:
    """Test that the impersonated headers are sampled from a single browser profile."""
    generator = Mock(spec=HeaderGenerator)
    generator.get_specific_headers.return_value = HttpHeaders(
        {'Accept': 'text/html', 'Accept-Language': 'en-GB', 'User-Agent': 'Mozilla/5.0 (Test)'}
    )

    async with HttpxHttpClient(header_generator=generator) as client:
        response = await client.send_request(str(server_url / 'headers'))
        headers = await read_json(response)

    assert headers['accept'] == 'text/html'
    assert headers['accept-language'] == 'en-GB'
    assert headers['user-agent'] == 'Mozilla/5.0 (Test)'
    generator.get_specific_headers.assert_called_once_with(header_names={'Accept', 'Accept-Language', 'User-Agent'})


async def test_no_headers_without_generator(server_url: URL) -> None:
    """Test that no browser-like headers are sent once the header generator is turned off."""
    async with HttpxHttpClient(header_generator=None) as client:
        response = await client.send_request(str(server_url / 'headers'))
        headers = await read_json(response)

    assert 'python-httpx' in headers['user-agent']
