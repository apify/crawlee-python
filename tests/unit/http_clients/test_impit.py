from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from impit import TooManyRedirects

from crawlee.http_clients import ImpitHttpClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL

    from crawlee._types import HttpMethod
    from crawlee.http_clients import HttpResponse


@pytest.fixture
async def http_client() -> AsyncGenerator[ImpitHttpClient]:
    client = ImpitHttpClient()
    async with client:
        yield client


async def read_json(response: HttpResponse) -> dict:
    return json.loads((await response.read()).decode())


@pytest.mark.parametrize(
    ('status_code', 'method', 'expected_method', 'expected_body'),
    [
        pytest.param(301, 'POST', 'GET', '', id='301-post'),
        pytest.param(301, 'PUT', 'PUT', 'payload', id='301-put'),
        pytest.param(302, 'POST', 'GET', '', id='302-post'),
        pytest.param(302, 'PUT', 'PUT', 'payload', id='302-put'),
        pytest.param(303, 'POST', 'GET', '', id='303-post'),
        pytest.param(303, 'PUT', 'GET', '', id='303-put'),
        pytest.param(307, 'POST', 'POST', 'payload', id='307-post'),
        pytest.param(308, 'PUT', 'PUT', 'payload', id='308-put'),
    ],
)
async def test_redirect_method(
    http_client: ImpitHttpClient,
    server_url: URL,
    *,
    status_code: int,
    method: HttpMethod,
    expected_method: str,
    expected_body: str,
) -> None:
    """Test that the method and the body of a redirected request follow the WHATWG Fetch algorithm."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'method'), status=status_code)

    response = await http_client.send_request(
        str(redirect_url),
        method=method,
        payload=b'payload',
        headers={'content-type': 'application/octet-stream'},
    )
    echo = await read_json(response)

    assert echo['method'] == expected_method
    assert echo['body'] == expected_body


async def test_body_headers_dropped(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that headers describing the request body are dropped once a redirect turns the request into a `GET`."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'headers'), status=302)

    response = await http_client.send_request(
        str(redirect_url),
        method='POST',
        payload=b'payload',
        headers={'content-type': 'application/json', 'content-language': 'uk', 'x-custom': 'kept'},
    )
    headers = await read_json(response)

    assert 'content-type' not in headers
    assert 'content-language' not in headers
    assert headers['x-custom'] == 'kept'


async def test_too_many_redirects(server_url: URL) -> None:
    """Test that an endless redirect chain is cut off by `max_redirects`."""
    async with ImpitHttpClient(max_redirects=2) as client:
        with pytest.raises(TooManyRedirects, match='limit of 2 redirects'):
            await client.send_request(str(server_url / 'redirect_loop'))
