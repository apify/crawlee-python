from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT_LANGUAGE
from crawlee.http_clients import HttpxHttpClient
from crawlee.sessions import Session

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL

    from crawlee.http_clients import HttpClient


@pytest.fixture
async def http_client() -> AsyncGenerator[HttpClient]:
    async with HttpxHttpClient(http2=False) as client:
        yield client


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

    assert 'user-agent' in response_headers
    assert 'python-httpx' not in response_headers['user-agent']
    assert response_headers['user-agent'] in get_available_header_values(header_network, {'User-Agent', 'user-agent'})


def test_headers_come_from_single_fingerprint() -> None:
    """Accept and User-Agent must come from one `generate()` call, not mixed profiles."""
    fingerprint = {'Accept': 'text/html', 'Accept-Language': 'en-US', 'User-Agent': 'TestAgent/1.0'}

    # Avoid HeaderGenerator.__init__ loading browserforge; only exercise get_specific_headers.
    header_generator = HeaderGenerator.__new__(HeaderGenerator)
    header_generator._generator = Mock()
    header_generator._generator.generate = Mock(return_value=fingerprint)

    client = HttpxHttpClient(header_generator=header_generator)
    combined = client._combine_headers(None)

    header_generator._generator.generate.assert_called_once()
    assert combined is not None
    assert combined['accept'] == 'text/html'
    assert combined['accept-language'] == 'en-US'
    assert combined['user-agent'] == 'TestAgent/1.0'


async def test_client_cache_is_shared_across_sessions(server_url: URL) -> None:
    """Distinct sessions must reuse one AsyncClient per proxy, not one client per cookie jar."""
    host = server_url.host
    assert host is not None

    client = HttpxHttpClient(http2=False)
    async with client:
        for i in range(5):
            session = Session()
            session.cookies.set(f'k{i}', f'v{i}', domain=host, path='/')
            await client.send_request(str(server_url / 'cookies'), session=session)

        assert len(client._client_by_proxy_url) == 1
