from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT_LANGUAGE
from crawlee.http_clients import HttpxHttpClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL

    from crawlee.http_clients import HttpClient


@pytest.fixture
async def http_client() -> AsyncGenerator[HttpClient]:
    async with HttpxHttpClient(http2=False) as client:
        yield client


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
