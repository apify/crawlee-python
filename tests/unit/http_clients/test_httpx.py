from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import httpx2
import pytest

from crawlee import HttpHeaders
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT_LANGUAGE
from crawlee.http_clients import HttpxHttpClient
from crawlee.http_clients._httpx import _HttpxTransport, _same_origin
from crawlee.sessions import CookieParam, Session

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo


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


async def test_proxy_kwarg_routes_requests(server_url: URL) -> None:
    """Test that a `proxy` kwarg routes the requests that carry no `ProxyInfo` of their own."""
    # Port 1 refuses every connection, so the request can only reach the server if the proxy is not used.
    async with HttpxHttpClient(proxy='http://127.0.0.1:1') as client:
        with pytest.raises(httpx2.ConnectError):
            await client.send_request(str(server_url / 'status/222'))


async def test_proxy_kwarg_works_against_a_real_proxy(proxy: ProxyInfo, server_url: URL) -> None:
    """Test that a `proxy` kwarg still gets the response through once the proxy accepts the connection."""
    async with HttpxHttpClient(proxy=proxy.url) as client:
        response = await client.send_request(str(server_url / 'status/222'))

    assert response.status_code == 222


async def test_proxy_info_wins_over_the_proxy_kwarg(proxy: ProxyInfo, server_url: URL) -> None:
    """Test that the `ProxyInfo` of a request takes precedence over the `proxy` kwarg of the client."""
    # Port 1 refuses every connection, so the request only succeeds if the `ProxyInfo` is the one being used.
    async with HttpxHttpClient(proxy='http://127.0.0.1:1') as client:
        response = await client.send_request(str(server_url / 'status/222'), proxy_info=proxy)

    assert response.status_code == 222


def test_silences_httpx_request_logging() -> None:
    """Instantiating the client lowers the noisy per-request `httpx2` INFO logs to WARNING by default."""
    httpx_logger = logging.getLogger('httpx2')
    httpx_logger.setLevel(logging.NOTSET)

    HttpxHttpClient()

    assert httpx_logger.level == logging.WARNING


async def test_common_headers_and_user_agent(server_url: URL, header_network: dict) -> None:
    """Test that the relevant headers use header values from header generator instead of default HTTPX2 headers.

    HTTPX2 uses own headers by default which is not desired as it could increase blocking chances.
    """
    client = HttpxHttpClient()

    response = await client.send_request(str(server_url / 'headers'))
    response_headers = json.loads((await response.read()).decode())

    assert 'accept' in response_headers
    assert response_headers['accept'] in get_available_header_values(header_network, {'Accept', 'accept'})

    assert 'accept-language' in response_headers
    assert response_headers['accept-language'] == COMMON_ACCEPT_LANGUAGE

    # By default, HTTPX2 uses its own User-Agent, which should be replaced by the one from the header generator.
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


async def test_client_cookie_header_wins_over_session(server_url: URL) -> None:
    """Test that a `Cookie` header set on the underlying client replaces the cookies of the session."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])

    async with HttpxHttpClient(headers={'cookie': 'from_client=1'}) as client:
        response = await client.send_request(str(server_url / 'cookies'), session=session)

    assert (await read_json(response))['cookies'] == {'from_client': '1'}


async def test_client_cookies_dropped_cross_origin(server_url: URL, redirect_server_url: URL) -> None:
    """Test that cookies of the underlying client reach their origin but not the target of a cross-origin redirect."""
    redirect_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'cookies'), status=302)

    async with HttpxHttpClient(cookies={'from_client': '1'}) as client:
        direct = await client.send_request(str(server_url / 'cookies'))
        redirected = await client.send_request(str(redirect_url))

    assert (await read_json(direct))['cookies'] == {'from_client': '1'}
    assert (await read_json(redirected))['cookies'] == {}


async def test_no_headers_without_generator(server_url: URL) -> None:
    """Test that no browser-like headers are sent once the header generator is turned off."""
    async with HttpxHttpClient(header_generator=None) as client:
        response = await client.send_request(str(server_url / 'headers'))
        headers = await read_json(response)

    assert 'python-httpx2' in headers['user-agent']
    assert headers['accept'] == '*/*'
    assert 'accept-language' not in headers
