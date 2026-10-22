from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from curl_cffi import CurlHttpVersion
from pydantic import ValidationError
from yarl import URL

from crawlee.errors import ProxyError
from crawlee.http_clients import CurlImpersonateHttpClient, HttpClient, HttpResponse, HttpxHttpClient, ImpitHttpClient
from crawlee.http_clients._base import _is_cross_origin, _redirect_method
from crawlee.http_clients._httpx import _HttpxResponse
from crawlee.http_clients._impit import _ImpitResponse
from crawlee.sessions import CookieParam, Session
from tests.unit.server import generate_file_content
from tests.unit.server_endpoints import HELLO_WORLD

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from _pytest.fixtures import SubRequest

    from crawlee._types import HttpMethod
    from crawlee.proxy_configuration import ProxyInfo


@pytest.fixture(
    params=[
        pytest.param(ImpitHttpClient, id='impit'),
        pytest.param(HttpxHttpClient, id='httpx'),
        pytest.param(CurlImpersonateHttpClient, id='curl'),
    ]
)
def http_client_class(request: SubRequest) -> type[HttpClient]:
    """Provide each HTTP client class, for tests that construct the client themselves."""
    return request.param


@pytest.fixture
async def custom_http_client(request: SubRequest) -> AsyncGenerator[HttpClient]:
    """Helper fixture to reduce code duplication.

    If clients are not initialized, create their default instances.
    Return client in active context, leave the context after the test."""

    client = request.param if isinstance(request.param, HttpClient) else request.param()
    async with client as _:
        yield _


async def read_json(response: HttpResponse) -> dict:
    return json.loads((await response.read()).decode())


async def test_send_request_zero_timeout_expires_immediately(http_client: HttpClient, server_url: URL) -> None:
    """A `timedelta(0)` timeout must expire immediately rather than being silently treated as no timeout."""
    slow_url = str((server_url / 'slow').with_query(delay=2))
    start = time.monotonic()

    with pytest.raises(TimeoutError):
        await http_client.send_request(slow_url, timeout=timedelta(0))

    assert time.monotonic() - start < 1


async def test_http_1(http_client: HttpClient, server_url: URL) -> None:
    response = await http_client.send_request(str(server_url))
    assert response.http_version == 'HTTP/1.1'


@pytest.mark.flaky(
    reruns=3,
    reason='`https://apify.com/` occasionally terminates the HTTP/2 connection with a GOAWAY '
    '(RemoteProtocolError / ConnectionTerminated); an external transient condition where a fresh '
    'rerun reconnects and negotiates HTTP/2 cleanly.',
)
@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(CurlImpersonateHttpClient(http_version=CurlHttpVersion.V2_0), id='curl'),
        pytest.param(HttpxHttpClient(http1=False, http2=True), id='httpx'),
        pytest.param(ImpitHttpClient(), id='impit'),
    ],
    indirect=['custom_http_client'],
)
async def test_http_2(custom_http_client: HttpClient) -> None:
    response = await custom_http_client.send_request('https://apify.com/')
    assert response.http_version == 'HTTP/2'


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy(
    http_client: HttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    url = str(server_url / 'status/222')

    response = await http_client.send_request(url, proxy_info=proxy)
    assert response.status_code == 222  # 222 - authentication successful


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy_disabled(
    http_client: HttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'

    with pytest.raises(ProxyError):
        await http_client.send_request(url, proxy_info=disabled_proxy)


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_session_cookies_sent_through_proxy(
    http_client: HttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    """Test that requests going through a proxy carry the session cookies and stay isolated per session."""
    session = Session(cookies=[CookieParam(name='jar', value='1', domain=server_url.host or '')])

    response = await http_client.send_request(str(server_url / 'cookies'), session=session, proxy_info=proxy)

    assert (await read_json(response))['cookies'] == {'jar': '1'}

    await http_client.send_request(
        str((server_url / 'set_cookies').with_query(a='1')),
        session=session,
        proxy_info=proxy,
    )

    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'jar', 'a'}

    other_session = Session()
    other_response = await http_client.send_request(
        str(server_url / 'cookies'),
        session=other_session,
        proxy_info=proxy,
    )

    assert (await read_json(other_response))['cookies'] == {}


async def test_response_url_after_redirect(http_client: HttpClient, server_url: URL) -> None:
    """Test that the `url` of a redirected response is the URL of the last request in the chain."""
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))

    response = await http_client.send_request(redirect_url)

    assert response.url == target_url


async def test_send_request_allow_redirects_by_default(http_client: HttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))

    response = await http_client.send_request(redirect_url)

    assert response.status_code == 200


async def test_ten_redirects_followed(http_client: HttpClient, server_url: URL) -> None:
    """Test that a redirect chain of exactly 10 redirects is followed to its end."""
    url = server_url / 'status/200'
    for _ in range(10):
        url = (server_url / 'redirect').with_query(url=str(url))

    response = await http_client.send_request(str(url))

    assert response.status_code == 200


async def test_too_many_redirects(http_client: HttpClient, server_url: URL) -> None:
    """Test that a redirect chain longer than 10 redirects raises an error."""
    with pytest.raises(RuntimeError, match=r'Too many redirects \(10\)'):
        await http_client.send_request(str(server_url / 'redirect_loop'))


async def test_timeout_covers_redirect_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that every request of a redirect chain gets what is left of the timeout, until it runs out."""
    url = 'https://example.com/'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': '/next'}, url=url, headers_set_cookie=[])
    fetch = AsyncMock(return_value=redirect)
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', fetch)
    # The deadline is set at 0, and the requests of the chain start at 0, 4 and 11 seconds.
    monkeypatch.setattr('crawlee.http_clients._base.monotonic', Mock(side_effect=[0, 0, 4, 11]))

    with pytest.raises(TimeoutError):
        await client.send_request(url, timeout=timedelta(seconds=10))

    timeouts = [fetch_call.kwargs['timeout'] for fetch_call in fetch.call_args_list]
    assert timeouts == [timedelta(seconds=10), timedelta(seconds=6)]


async def test_redirect_responses_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that every response of a redirect chain is closed except the returned one."""
    url = 'https://example.com/'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': '/next'}, url=url, headers_set_cookie=[])
    final = Mock(spec=HttpResponse, status_code=200, headers={}, url=f'{url}next', headers_set_cookie=[])
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', AsyncMock(side_effect=[redirect, final]))

    response = await client.send_request(url)

    assert response is final
    redirect.aclose.assert_awaited_once()
    final.aclose.assert_not_awaited()


async def test_redirect_response_closed_on_invalid_location(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a redirect response whose `Location` can't be parsed is closed before the error propagates."""
    url = 'https://example.com/'
    location = 'http://[invalid'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': location}, url=url, headers_set_cookie=[])
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', AsyncMock(return_value=redirect))

    with pytest.raises(ValueError, match='IPv6'):
        await client.send_request(url)

    redirect.aclose.assert_awaited_once()


@pytest.mark.parametrize(
    ('path', 'query', 'expected_status_code'),
    [
        pytest.param('status/302', {}, 302, id='missing-location'),
        pytest.param('redirect', {'url': 'ftp://127.0.0.1/file'}, 302, id='non-http-location'),
        pytest.param('redirect', {'url': '/status/200', 'status': '300'}, 300, id='non-redirect-status'),
    ],
)
async def test_redirect_not_followed(
    http_client: HttpClient, server_url: URL, path: str, query: dict[str, str], expected_status_code: int
) -> None:
    """Test that a 3xx without a `Location`, to a non-HTTP scheme or of a non-redirect status is returned as it is."""
    response = await http_client.send_request(str((server_url / path).with_query(query)))

    assert response.status_code == expected_status_code


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
    http_client: HttpClient,
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


def test_see_other_keeps_head() -> None:
    """Test that a `303` redirect of a `HEAD` request is followed with `HEAD`."""
    assert _redirect_method(303, 'HEAD') == 'HEAD'


async def test_body_headers_dropped(http_client: HttpClient, server_url: URL) -> None:
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


@pytest.mark.parametrize(
    ('url', 'other', 'expected'),
    [
        pytest.param('http://a.com/x', 'http://a.com/x', False, id='same'),
        pytest.param('http://a.com/x', 'https://a.com/x', True, id='different-scheme'),
        pytest.param('http://a.com/x', 'http://b.com/x', True, id='different-host'),
        pytest.param('http://A.com/x', 'http://a.com/y', False, id='different-host-case'),
        pytest.param('http://a.com/x', 'http://a.com:8080/x', True, id='different-port'),
        pytest.param('http://a.com:80/x', 'http://a.com/y', False, id='explicit-default-port'),
        pytest.param('http://a.com/x', 'http://a.com/y', False, id='different-path'),
    ],
)
def test_is_cross_origin(url: str, other: str, *, expected: bool) -> None:
    """Test that a redirect leaves the origin when the scheme, host or port changes."""
    assert _is_cross_origin(URL(url, encoded=True), URL(other, encoded=True)) is expected


async def test_read_stream(http_client: HttpClient, server_url: URL) -> None:
    """Test that `read_stream` yields the whole body."""
    content_body: bytes = b''

    response = await http_client.send_request(str(server_url))
    assert response.status_code == 200
    async for chunk in response.read_stream():
        content_body += chunk

    assert content_body == HELLO_WORLD


async def test_read_stream_after_transfer_finished(http_client: HttpClient, server_url: URL) -> None:
    """A small body can be fully buffered before `read_stream` is called. It must still be readable."""
    file_size = 16
    content_body: bytes = b''

    small_file_url = str((server_url / 'file').update_query(size=file_size))

    response = await http_client.send_request(small_file_url)
    # Guarantee at least one event-loop tick for the response to be buffered.
    await asyncio.sleep(0)

    async for chunk in response.read_stream():
        content_body += chunk

    assert content_body == generate_file_content(file_size)


async def test_read_stream_with_empty_body(http_client: HttpClient, server_url: URL) -> None:
    """A response with an empty body must be readable as a stream and yield no chunks."""
    content_body: bytes = b''
    response = await http_client.send_request(str(server_url / 'status/200'))
    assert response.status_code == 200
    async for chunk in response.read_stream():
        content_body += chunk

    assert content_body == b''


async def test_read_stream_twice_raises(http_client: HttpClient, server_url: URL) -> None:
    """Test that a body consumed by `read_stream` can't be streamed again."""
    response = await http_client.send_request(str(server_url))
    content_body_first: bytes = b''
    async for chunk in response.read_stream():
        content_body_first += chunk

    with pytest.raises(RuntimeError):
        [chunk async for chunk in response.read_stream()]

    assert content_body_first == HELLO_WORLD


async def test_read_after_read_stream_raises(http_client: HttpClient, server_url: URL) -> None:
    """Test that a body consumed by `read_stream` can't be read."""
    response = await http_client.send_request(str(server_url))
    [chunk async for chunk in response.read_stream()]

    with pytest.raises(RuntimeError):
        await response.read()


async def test_read_stream_after_read_raises(http_client: HttpClient, server_url: URL) -> None:
    """Test that a body consumed by `read` can't be streamed."""
    response = await http_client.send_request(str(server_url))
    await response.read()

    with pytest.raises(RuntimeError):
        [chunk async for chunk in response.read_stream()]


async def test_response_context_manager_closes_response(http_client: HttpClient, server_url: URL) -> None:
    """Test that leaving the context of a response closes it."""
    response = await http_client.send_request(str(server_url))

    with patch.object(response, 'aclose', wraps=response.aclose) as aclose:
        async with response as entered:
            assert entered is response
            aclose.assert_not_called()

    aclose.assert_awaited_once()


async def test_read_twice_returns_same_body(http_client: HttpClient, server_url: URL) -> None:
    """Test that repeated `read` calls return the same body."""
    response = await http_client.send_request(str(server_url))

    assert await response.read() == HELLO_WORLD
    assert await response.read() == HELLO_WORLD


async def test_read_again_after_timeout(http_client: HttpClient, server_url: URL) -> None:
    """Test that a body read within the timeout is returned again by `read` once the timeout has passed."""
    response = await http_client.send_request(str(server_url), timeout=timedelta(milliseconds=200))
    body = await response.read()

    await asyncio.sleep(0.3)

    assert await response.read() == body


async def test_aclose_is_idempotent(http_client: HttpClient, server_url: URL) -> None:
    """Test that closing a response a second time has no effect."""
    response = await http_client.send_request(str(server_url))

    await response.aclose()
    await response.aclose()


@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(CurlImpersonateHttpClient(), id='curl'),
        pytest.param(HttpxHttpClient(), id='httpx'),
        pytest.param(ImpitHttpClient(), id='impit'),
    ],
)
async def test_reuse_context_manager(custom_http_client: HttpClient, server_url: URL) -> None:
    async with custom_http_client:
        response = await custom_http_client.send_request(str(server_url))
        assert response.status_code == 200

    # Reusing the context manager should not raise an error
    async with custom_http_client:
        response = await custom_http_client.send_request(str(server_url))
        assert response.status_code == 200


async def test_work_after_cleanup(http_client: HttpClient, server_url: URL) -> None:
    response = await http_client.send_request(str(server_url))
    assert response.status_code == 200

    # Cleanup the client
    await http_client.cleanup()

    # After cleanup, the client should still work
    response = await http_client.send_request(str(server_url))
    assert response.status_code == 200


async def test_compressed_chunked_read_stream(http_client: HttpClient, server_url: URL) -> None:
    """Test that `read_stream` yields a compressed body decompressed."""
    content_body: bytes = b''

    response = await http_client.send_request(str(server_url / 'get_compressed'))
    assert response.status_code == 200
    async for chunk in response.read_stream():
        content_body += chunk

    assert content_body == HELLO_WORLD * 1000


async def test_send_request_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        await http_client.send_request('gopher://127.0.0.1:6379/_PING')


@pytest.mark.parametrize(
    ('optional_module_name', 'import_path'),
    [
        pytest.param('curl_cffi', 'crawlee.http_clients._curl_impersonate', id='curl_impersonate'),
        pytest.param('httpx2', 'crawlee.http_clients._httpx', id='httpx'),
    ],
)
def test_import_error_handled(optional_module_name: str, import_path: str) -> None:
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith(import_path):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            importlib.import_module(import_path)


async def test_sessions_share_one_client(http_client: HttpClient, server_url: URL) -> None:
    """Test that requests of different sessions are served by a single underlying client."""
    for _ in range(3):
        await http_client.send_request(str(server_url / 'cookies'), session=Session())

    assert len(http_client._client_by_proxy_url) == 1  # ty: ignore[unresolved-attribute]


async def test_cookies_isolated_per_session(http_client: HttpClient, server_url: URL) -> None:
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


async def test_cookies_collected_on_redirect(http_client: HttpClient, server_url: URL) -> None:
    """Test that a cookie set by a redirecting response is sent on the following hop."""
    session = Session()

    response = await http_client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=session)

    assert (await read_json(response))['cookies'] == {'a': '1'}


async def test_cookies_kept_for_redirect_chain_without_session(http_client: HttpClient, server_url: URL) -> None:
    """Test that without a session the cookies set during a redirect chain reach its next hops, but no other call."""
    response = await http_client.send_request(str((server_url / 'set_cookies').with_query(a='1')))
    other_response = await http_client.send_request(str(server_url / 'cookies'))

    assert (await read_json(response))['cookies'] == {'a': '1'}
    assert (await read_json(other_response))['cookies'] == {}


async def test_headers_set_cookie_lists_every_header(http_client: HttpClient, server_url: URL) -> None:
    """Test that `headers_set_cookie` holds every `Set-Cookie` header of the response."""
    response = await http_client.send_request(str(server_url / 'set_complex_cookies'))

    names = [header.partition('=')[0] for header in response.headers_set_cookie]
    assert names == ['basic', 'withpath', 'strict', 'secure', 'short', 'domain']


async def test_every_response_cookie_stored(http_client: HttpClient, server_url: URL) -> None:
    """Test that every cookie of a response setting several cookies is stored in the session."""
    session = Session()

    await http_client.send_request(str((server_url / 'set_cookies').with_query(a='1', b='2', c='3')), session=session)

    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'a', 'b', 'c'}


async def test_cookies_of_non_ascii_host_sent_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a host with a non-ASCII name is requested in its IDNA form and gets its cookies back."""
    ascii_url = 'http://xn--bcher-kva.example/'
    response = Mock(spec=HttpResponse, status_code=200, headers={}, url=ascii_url, headers_set_cookie=['sid=1'])
    fetch = AsyncMock(return_value=response)
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', fetch)
    session = Session()

    await client.send_request('http://b\u00fccher.example/', session=session)
    await client.send_request('http://b\u00fccher.example/', session=session)

    assert fetch.call_args.args[0] == ascii_url
    assert fetch.call_args.kwargs['headers']['cookie'] == 'sid=1'


@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(lambda: CurlImpersonateHttpClient(persist_cookies_per_session=False), id='curl'),
        pytest.param(lambda: HttpxHttpClient(persist_cookies_per_session=False), id='httpx'),
        pytest.param(lambda: ImpitHttpClient(persist_cookies_per_session=False), id='impit'),
    ],
    indirect=['custom_http_client'],
)
async def test_cookies_not_persisted(custom_http_client: HttpClient, server_url: URL) -> None:
    """Test that `persist_cookies_per_session` keeps the session jar untouched."""
    session = Session()

    await custom_http_client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=session)

    assert session.cookies.get_cookies_as_dicts() == []


@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(lambda: CurlImpersonateHttpClient(persist_cookies_per_session=False), id='curl'),
        pytest.param(lambda: HttpxHttpClient(persist_cookies_per_session=False), id='httpx'),
        pytest.param(lambda: ImpitHttpClient(persist_cookies_per_session=False), id='impit'),
    ],
    indirect=['custom_http_client'],
)
async def test_cookies_sent_when_not_persisted(custom_http_client: HttpClient, server_url: URL) -> None:
    """Test that `persist_cookies_per_session` gates storing the response cookies, not sending the session ones."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])

    response = await custom_http_client.send_request(str(server_url / 'cookies'), session=session)

    assert (await read_json(response))['cookies'] == {'from_jar': '1'}


async def test_redirect_cookies_sent_when_not_persisted(server_url: URL, http_client_class: type[HttpClient]) -> None:
    """Test that with `persist_cookies_per_session` off the cookies set during a redirect chain reach its next hops."""
    session = Session()

    async with http_client_class(persist_cookies_per_session=False) as client:
        response = await client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=session)
        cookies = (await read_json(response))['cookies']

    assert cookies == {'a': '1'}
    assert session.cookies.get_cookies_as_dicts() == []


async def test_cookie_header_rebuilt_per_hop(http_client: HttpClient, server_url: URL) -> None:
    """Test that the `Cookie` header of one hop does not reach a hop whose URL the cookie does not match."""
    session = Session(
        cookies=[CookieParam(name='scoped', value='value', domain=server_url.host or '', path='/redirect')]
    )

    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)
    response = await http_client.send_request(str(redirect_url), session=session)

    assert (await read_json(response))['cookies'] == {}
    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'scoped'}


async def test_cookie_header_wins_over_session(http_client: HttpClient, server_url: URL) -> None:
    """Test that a `Cookie` header passed by the caller replaces the cookies of the session."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])

    response = await http_client.send_request(
        str(server_url / 'cookies'),
        session=session,
        headers={'cookie': 'manual=value'},
    )

    assert (await read_json(response))['cookies'] == {'manual': 'value'}


async def test_cookie_header_not_stored_in_session(http_client: HttpClient, server_url: URL) -> None:
    """Test that the cookies of the `Cookie` header of the caller are not stored in the session."""
    session = Session()

    await http_client.send_request(str(server_url / 'cookies'), session=session, headers={'cookie': 'manual=value'})

    assert session.cookies.get_cookies_as_dicts() == []


async def test_cookie_header_kept_same_origin(http_client: HttpClient, server_url: URL) -> None:
    """Test that a `Cookie` header set by the caller survives a redirect within the origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'cookie': 'manual=value'})

    assert (await read_json(response))['cookies'] == {'manual': 'value'}


async def test_cookie_header_dropped_cross_origin(
    http_client: HttpClient,
    server_url: URL,
    redirect_server_url: URL,
) -> None:
    """Test that a `Cookie` header set by the caller is dropped once a redirect leaves the origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'cookies'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'cookie': 'manual=value'})

    assert (await read_json(response))['cookies'] == {}


async def test_session_cookies_take_over_cross_origin(
    http_client: HttpClient,
    server_url: URL,
    redirect_server_url: URL,
) -> None:
    """Test that the session cookies of the new origin take over once a redirect leaves the origin of the caller."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=redirect_server_url.host or '')])
    redirect_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'cookies'), status=302)

    response = await http_client.send_request(
        str(redirect_url),
        session=session,
        headers={'cookie': 'manual=value'},
    )

    assert (await read_json(response))['cookies'] == {'from_jar': '1'}


async def test_cookie_header_wins_over_session_on_redirect(http_client: HttpClient, server_url: URL) -> None:
    """Test that a `Cookie` header of the caller keeps beating the session cookies after a redirect."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)

    response = await http_client.send_request(
        str(redirect_url),
        session=session,
        headers={'cookie': 'manual=value'},
    )

    assert (await read_json(response))['cookies'] == {'manual': 'value'}


async def test_empty_cookie_header_suppresses_session_cookies(http_client: HttpClient, server_url: URL) -> None:
    """Test that an empty `Cookie` header of the caller keeps the session cookies out of every hop."""
    session = Session(cookies=[CookieParam(name='from_jar', value='1', domain=server_url.host or '')])
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)

    direct = await http_client.send_request(str(server_url / 'cookies'), session=session, headers={'cookie': ''})
    redirected = await http_client.send_request(str(redirect_url), session=session, headers={'cookie': ''})

    assert (await read_json(direct))['cookies'] == {}
    assert (await read_json(redirected))['cookies'] == {}


async def test_auth_kept_same_origin(http_client: HttpClient, server_url: URL) -> None:
    """Test that credentials survive a redirect that stays on the same origin."""
    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'headers'), status=302)

    response = await http_client.send_request(str(redirect_url), headers={'authorization': 'Bearer token'})
    headers = await read_json(response)

    assert headers['authorization'] == 'Bearer token'


async def test_auth_dropped_cross_origin(
    http_client: HttpClient,
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


async def test_send_request_keeps_cookies_and_encoding(http_client: HttpClient, server_url: URL) -> None:
    """Test that session cookies survive a redirect and that signed URLs are sent without re-encoding."""
    session = Session(cookies=[CookieParam(name='preset', value='value', domain=server_url.host or '')])

    signed_query = 'X-Amz-Credential=AKIA%2F20240101%2Fus-east-1&X-Amz-Date=2024-01-01T00%3A00%3A00Z'
    target_url = f'{server_url / "cookies"}?{signed_query}'

    direct = await http_client.send_request(target_url, session=session)

    assert (await read_json(direct))['cookies'] == {'preset': 'value'}
    assert direct.url == target_url

    redirect_url = str((server_url / 'redirect').with_query(url=target_url, status=302))
    redirected = await http_client.send_request(redirect_url, session=session)

    assert (await read_json(redirected))['cookies'] == {'preset': 'value'}
    assert redirected.url == target_url


@pytest.mark.parametrize(
    ('client_class', 'kwarg'),
    [
        pytest.param(HttpxHttpClient, 'cookies', id='httpx_cookies'),
        pytest.param(HttpxHttpClient, 'auth', id='httpx_auth'),
        pytest.param(HttpxHttpClient, 'params', id='httpx_params'),
        pytest.param(CurlImpersonateHttpClient, 'auth', id='curl_auth'),
        pytest.param(CurlImpersonateHttpClient, 'params', id='curl_params'),
        pytest.param(CurlImpersonateHttpClient, 'async_curl', id='curl_async_curl'),
    ],
)
def test_ignored_argument_warns(client_class: type[HttpClient], kwarg: str) -> None:
    """Test that an argument of the underlying library that the client ignores raises a warning."""
    with pytest.warns(UserWarning, match=f'`{kwarg}` argument'):
        client_class(**{kwarg: Mock()})


@pytest.mark.parametrize(
    ('client_class', 'kwargs'),
    [
        pytest.param(ImpitHttpClient, {'max_redirects': 0}, id='impit_max_redirects'),
        pytest.param(HttpxHttpClient, {'max_redirects': 0}, id='httpx_max_redirects'),
        pytest.param(CurlImpersonateHttpClient, {'max_redirects': 0}, id='curl_max_redirects'),
        pytest.param(CurlImpersonateHttpClient, {'allow_redirects': False}, id='curl_allow_redirects'),
    ],
)
async def test_library_redirect_options_ignored(
    server_url: URL,
    client_class: type[HttpClient],
    kwargs: dict[str, Any],
) -> None:
    """Test that the redirect options of the underlying library raise a warning and redirects are still followed."""
    with pytest.warns(UserWarning, match='argument'):
        client = client_class(**kwargs)

    redirect_url = (server_url / 'redirect').with_query(url=str(server_url / 'cookies'), status=302)

    async with client:
        response = await client.send_request(str(redirect_url))

    assert response.status_code == 200


@pytest.mark.parametrize(
    ('follow_redirects', 'expected_status_code'),
    [
        pytest.param(True, 200, id='followed'),
        pytest.param(False, 302, id='not_followed'),
    ],
)
async def test_follow_redirects(
    server_url: URL,
    http_client_class: type[HttpClient],
    *,
    follow_redirects: bool,
    expected_status_code: int,
) -> None:
    """Test that `follow_redirects` decides whether a redirect is followed, while its cookies are stored either way."""
    session = Session()

    async with http_client_class(follow_redirects=follow_redirects) as client:
        response = await client.send_request(str((server_url / 'set_cookies').with_query(a='1')), session=session)

        assert response.status_code == expected_status_code

    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'a'}


async def test_closing_unread_response_reuses_connection(http_client_class: type[HttpClient]) -> None:
    """Test that a redirect hop and a response closed unread keep their connection when their body is small."""
    connection_opened = Mock()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection_opened()
        try:
            while True:
                request_head = await reader.readuntil(b'\r\n\r\n')
                if request_head.startswith(b'GET /start '):
                    writer.write(b'HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 5\r\n\r\n')
                    body = b'moved'
                else:
                    writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n')
                    body = b'ok'
                await writer.drain()
                # The body arrives after the headers, so a response closed right away hasn't received it yet.
                await asyncio.sleep(0.05)
                writer.write(body)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]

    async with server, http_client_class() as client:
        # `send_request` closes the redirect hop, and the final response is closed without reading it.
        response = await client.send_request(f'http://127.0.0.1:{port}/start')
        await response.aclose()

        response = await client.send_request(f'http://127.0.0.1:{port}/final')
        async with response:
            assert await response.read() == b'ok'

    connection_opened.assert_called_once()


@pytest.mark.parametrize(
    'response_class',
    [
        pytest.param(_ImpitResponse, id='impit'),
        pytest.param(_HttpxResponse, id='httpx'),
    ],
)
async def test_aclose_closes_when_cancelled_while_reading(
    response_class: type[_ImpitResponse | _HttpxResponse],
) -> None:
    """Test that `aclose` closes the response of the library when it is cancelled while reading a small body."""
    library_response = Mock(headers={'content-length': '10'})
    library_response.aread = AsyncMock(side_effect=asyncio.CancelledError)
    library_response.aclose = AsyncMock()
    response = response_class(library_response, deadline=None)

    with pytest.raises(asyncio.CancelledError):
        await response.aclose()

    library_response.aclose.assert_awaited_once()


async def test_client_headers_sent(server_url: URL, http_client_class: type[HttpClient]) -> None:
    """Test that the headers given to the client are sent, and the headers of a request win over them."""
    async with http_client_class(headers={'x-client': 'client', 'x-both': 'client'}) as client:
        response = await client.send_request(str(server_url / 'headers'), headers={'x-both': 'request'})
        headers = await read_json(response)

    assert headers['x-client'] == 'client'
    assert headers['x-both'] == 'request'


async def test_client_auth_header_dropped_cross_origin(
    server_url: URL,
    redirect_server_url: URL,
    http_client_class: type[HttpClient],
) -> None:
    """Test that an `Authorization` header given to the client is dropped as soon as a redirect leaves the origin."""
    same_origin_url = (server_url / 'redirect').with_query(url=str(server_url / 'headers'), status=302)
    cross_origin_url = (server_url / 'redirect').with_query(url=str(redirect_server_url / 'headers'), status=302)

    async with http_client_class(headers={'authorization': 'Bearer token'}) as client:
        same_origin_headers = await read_json(await client.send_request(str(same_origin_url)))
        cross_origin_headers = await read_json(await client.send_request(str(cross_origin_url)))

    assert same_origin_headers['authorization'] == 'Bearer token'
    assert 'authorization' not in cross_origin_headers


async def test_timeout_bounds_waiting_for_headers(http_client: HttpClient, server_url: URL) -> None:
    """Test that the timeout is a wall-clock deadline, even for a client that rounds its own timeout up."""
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        await http_client.send_request(
            str((server_url / 'slow').with_query(delay=3)),
            timeout=timedelta(milliseconds=200),
        )

    assert time.monotonic() - started < 0.8


async def test_timeout_bounds_read(http_client: HttpClient, server_url: URL) -> None:
    """Test that the timeout of `send_request` also bounds reading the body with `read`."""
    url = str((server_url / 'slow_body').with_query(chunks=50, delay=0.1))

    async with await http_client.send_request(url, timeout=timedelta(milliseconds=500)) as response:
        with pytest.raises(TimeoutError):
            await response.read()


async def test_timeout_bounds_read_stream(http_client: HttpClient, server_url: URL) -> None:
    """Test that the timeout of `send_request` also bounds reading the body with `read_stream`."""
    url = str((server_url / 'slow_body').with_query(chunks=50, delay=0.1))

    async with await http_client.send_request(url, timeout=timedelta(milliseconds=500)) as response:
        with pytest.raises(TimeoutError):
            async for _ in response.read_stream():
                pass
