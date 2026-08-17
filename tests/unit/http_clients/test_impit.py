from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from impit import TooManyRedirects

from crawlee import Request
from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import CookieParam, Session

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
    """Read the body of an HTTP response and decode it as JSON."""
    return json.loads((await response.read()).decode())


async def test_sessions_share_one_client(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that requests of different sessions are served by a single underlying client."""
    for _ in range(3):
        await http_client.send_request(str(server_url / 'cookies'), session=Session())

    assert len(http_client._client_by_proxy_url) == 1


async def test_cookies_isolated_per_session(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that sessions sharing a client don't see cookies of each other."""
    first_session = Session()
    second_session = Session()

    await http_client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=first_session)
    await http_client.send_request(str((server_url / 'set_cookies').with_query(b='2')), session=second_session)

    assert {item['name'] for item in first_session.cookies.get_cookies_as_dicts()} == {'a'}
    assert {item['name'] for item in second_session.cookies.get_cookies_as_dicts()} == {'b'}

    first_response = await http_client.send_request(str(server_url / 'cookies'), session=first_session)
    second_response = await http_client.send_request(str(server_url / 'cookies'), session=second_session)

    assert (await read_json(first_response))['cookies'] == {'a': '1'}
    assert (await read_json(second_response))['cookies'] == {'b': '2'}


async def test_cookies_collected_on_redirect(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that a cookie set by a redirecting response is sent on the following hop."""
    session = Session()

    response = await http_client.send_request(
        str((server_url / 'set_cookies').with_query(a='1')),
        session=session,
    )

    assert (await read_json(response))['cookies'] == {'a': '1'}


async def test_cookies_not_persisted(server_url: URL) -> None:
    """Test that `persist_cookies_per_session` keeps the session jar untouched."""
    session = Session()

    async with ImpitHttpClient(persist_cookies_per_session=False) as client:
        await client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=session)

    assert session.cookies.get_cookies_as_dicts() == []


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


async def test_auth_kept_same_origin(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that credentials survive a redirect that stays on the same origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'headers'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'authorization': 'Bearer token'})
    headers = await read_json(response)

    assert headers['authorization'] == 'Bearer token'


async def test_auth_dropped_cross_origin(
    http_client: ImpitHttpClient,
    server_url: URL,
    redirect_server_url: URL,
) -> None:
    """Test that credentials are dropped as soon as a redirect leaves the origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'headers'), status=302)

    response = await http_client.send_request(
        str(redirect_url),
        headers={'authorization': 'Bearer token', 'x-custom': 'kept'},
    )
    headers = await read_json(response)

    assert 'authorization' not in headers
    assert headers['x-custom'] == 'kept'


async def test_cookie_header_kept_same_origin(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that a `Cookie` header set by the caller survives a redirect within the origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'cookie': 'manual=value'})

    assert (await read_json(response))['cookies'] == {'manual': 'value'}


async def test_cookie_header_rebuilt_per_hop(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that the `Cookie` header of one hop does not reach a hop whose URL the cookie does not match."""
    session = Session(
        cookies=[CookieParam(name='scoped', value='value', domain=server_url.host or '', path='/redirect')]
    )

    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)
    response = await http_client.send_request(str(redirect_url), session=session)

    assert (await read_json(response))['cookies'] == {}
    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'scoped'}


async def test_cookie_header_wins_over_session(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that a `Cookie` header passed by the caller replaces the cookies of the session, as `impit` does."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])

    response = await http_client.send_request(
        str(server_url / 'cookies'),
        session=session,
        headers={'cookie': 'manual=value'},
    )

    assert (await read_json(response))['cookies'] == {'manual': 'value'}


async def test_cookie_header_dropped_cross_origin(
    http_client: ImpitHttpClient,
    server_url: URL,
    redirect_server_url: URL,
) -> None:
    """Test that a `Cookie` header set by the caller is dropped once a redirect leaves the origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'cookies'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'cookie': 'manual=value'})

    assert (await read_json(response))['cookies'] == {}


async def test_too_many_redirects(server_url: URL) -> None:
    """Test that an endless redirect chain is cut off by `max_redirects`."""
    async with ImpitHttpClient(max_redirects=2) as client:
        with pytest.raises(TooManyRedirects, match='limit of 2 redirects'):
            await client.send_request(str(server_url / 'redirect_loop'))


async def test_stream_follows_redirects(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that streamed requests follow redirects and carry session cookies along."""
    session = Session()
    stream_url = (server_url / 'set_cookies').with_query(a='1')

    async with http_client.stream(str(stream_url), session=session) as response:
        content = b''
        async for chunk in response.read_stream():
            content += chunk

    assert json.loads(content.decode())['cookies'] == {'a': '1'}
    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'a'}


async def test_crawl_keeps_cookies_and_encoding(http_client: ImpitHttpClient, server_url: URL) -> None:
    """Test that `crawl` carries session cookies through a redirect and sends signed URLs without re-encoding."""
    session = Session(cookies=[CookieParam(name='preset', value='value', domain=server_url.host or '')])

    signed_query = 'X-Amz-Credential=AKIA%2F20240101%2Fus-east-1&X-Amz-Date=2024-01-01T00%3A00%3A00Z'
    target_url = f'{server_url / "cookies"}?{signed_query}'

    direct_request = Request.from_url(target_url)
    direct_result = await http_client.crawl(direct_request, session=session)

    assert json.loads((await direct_result.http_response.read()).decode())['cookies'] == {'preset': 'value'}
    assert direct_request.loaded_url == target_url

    redirected_request = Request.from_url(str((server_url / 'redirect').with_query(url=target_url, status=302)))
    redirected_result = await http_client.crawl(redirected_request, session=session)

    assert json.loads((await redirected_result.http_response.read()).decode())['cookies'] == {'preset': 'value'}
    assert redirected_request.loaded_url == target_url
