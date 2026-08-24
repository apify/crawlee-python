from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import time
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from curl_cffi import CurlHttpVersion
from pydantic import ValidationError

from crawlee import Request
from crawlee.errors import ProxyError
from crawlee.http_clients import CurlImpersonateHttpClient, HttpClient, HttpxHttpClient, ImpitHttpClient
from crawlee.sessions import CookieParam, Session
from crawlee.statistics import Statistics
from tests.unit.server import generate_file_content
from tests.unit.server_endpoints import HELLO_WORLD

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from _pytest.fixtures import SubRequest
    from yarl import URL

    from crawlee.http_clients import HttpResponse
    from crawlee.proxy_configuration import ProxyInfo


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

    with pytest.raises(asyncio.TimeoutError):
        await http_client.send_request(slow_url, timeout=timedelta(0))

    assert time.monotonic() - start < 1


async def test_crawl_zero_timeout_expires_immediately(http_client: HttpClient, server_url: URL) -> None:
    """A `timedelta(0)` timeout must expire immediately rather than being silently treated as no timeout."""
    slow_url = str((server_url / 'slow').with_query(delay=2))
    start = time.monotonic()

    with pytest.raises(asyncio.TimeoutError):
        await http_client.crawl(Request.from_url(slow_url), timeout=timedelta(0))

    assert time.monotonic() - start < 1


async def test_stream_zero_timeout_expires_immediately(http_client: HttpClient, server_url: URL) -> None:
    """A `timedelta(0)` timeout must expire immediately rather than being silently treated as no timeout."""
    slow_url = str((server_url / 'slow').with_query(delay=2))
    start = time.monotonic()

    with pytest.raises(asyncio.TimeoutError):
        async with http_client.stream(slow_url, timeout=timedelta(0)):
            pass

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
async def test_crawl_with_proxy(
    http_client: HttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    url = str(server_url / 'status/222')
    request = Request.from_url(url)

    async with Statistics.with_default_state() as statistics:
        result = await http_client.crawl(request, proxy_info=proxy, statistics=statistics)

    assert result.http_response.status_code == 222  # 222 - authentication successful


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_crawl_with_proxy_disabled(
    http_client: HttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'
    request = Request.from_url(url)

    with pytest.raises(ProxyError):
        async with Statistics.with_default_state() as statistics:
            await http_client.crawl(request, proxy_info=disabled_proxy, statistics=statistics)


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

    request = Request.from_url(str(server_url / 'cookies'))
    crawling_result = await http_client.crawl(request, session=session, proxy_info=proxy)

    assert (await read_json(crawling_result.http_response))['cookies'] == {'jar': '1'}

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


async def test_crawl_allow_redirects_by_default(http_client: HttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)
    crawling_result = await http_client.crawl(request)

    assert crawling_result.http_response.status_code == 200
    assert request.loaded_url == target_url


@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(CurlImpersonateHttpClient(allow_redirects=False), id='curl'),
        pytest.param(HttpxHttpClient(follow_redirects=False), id='httpx'),
        pytest.param(ImpitHttpClient(follow_redirects=False), id='impit'),
    ],
    indirect=['custom_http_client'],
)
async def test_crawl_allow_redirects_false(custom_http_client: HttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)

    crawling_result = await custom_http_client.crawl(request)

    assert crawling_result.http_response.status_code == 302
    assert crawling_result.http_response.headers['Location'] == target_url
    assert request.loaded_url == redirect_url


async def test_send_request_allow_redirects_by_default(http_client: HttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))

    response = await http_client.send_request(redirect_url)

    assert response.status_code == 200


@pytest.mark.parametrize(
    'custom_http_client',
    [
        pytest.param(CurlImpersonateHttpClient(allow_redirects=False), id='curl'),
        pytest.param(HttpxHttpClient(follow_redirects=False), id='httpx'),
        pytest.param(ImpitHttpClient(follow_redirects=False), id='impit'),
    ],
    indirect=['custom_http_client'],
)
async def test_send_request_allow_redirects_false(custom_http_client: HttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))

    response = await custom_http_client.send_request(redirect_url)

    assert response.status_code == 302
    assert response.headers['Location'] == target_url


async def test_stream(http_client: HttpClient, server_url: URL) -> None:
    content_body: bytes = b''

    async with http_client.stream(str(server_url)) as response:
        assert response.status_code == 200
        async for chunk in response.read_stream():
            content_body += chunk

    assert content_body == HELLO_WORLD


async def test_stream_read_after_transfer_finished(http_client: HttpClient, server_url: URL) -> None:
    """A small body can be fully buffered before `read_stream` is called. It must still be readable."""
    file_size = 16
    content_body: bytes = b''

    small_file_url = str((server_url / 'file').update_query(size=file_size))

    async with http_client.stream(small_file_url) as response:
        # Guarantee at least one event-loop tick for the response to be buffered.
        await asyncio.sleep(0)

        async for chunk in response.read_stream():
            content_body += chunk

    assert content_body == generate_file_content(file_size)


async def test_stream_with_empty_body(http_client: HttpClient, server_url: URL) -> None:
    """A streamed response with an empty body must be readable and yield no chunks."""
    content_body: bytes = b''
    async with http_client.stream(str(server_url / 'status/200')) as response:
        assert response.status_code == 200
        async for chunk in response.read_stream():
            content_body += chunk

    assert content_body == b''


async def test_stream_error_double_read_stream(http_client: HttpClient, server_url: URL) -> None:
    async with http_client.stream(str(server_url)) as response:
        assert response.status_code == 200
        content_body_first: bytes = b''
        async for chunk in response.read_stream():
            content_body_first += chunk

        with pytest.raises(RuntimeError):
            [chunk async for chunk in response.read_stream()]

    assert content_body_first == HELLO_WORLD


async def test_stream_error_for_read(http_client: HttpClient, server_url: URL) -> None:
    async with http_client.stream(str(server_url)) as response:
        assert response.status_code == 200

        with pytest.raises(RuntimeError):
            await response.read()


async def test_send_request_error_for_read_stream(http_client: HttpClient, server_url: URL) -> None:
    response = await http_client.send_request(str(server_url))

    assert response.status_code == 200
    with pytest.raises(RuntimeError):
        [item async for item in response.read_stream()]


async def test_send_crawl_error_for_read_stream(http_client: HttpClient, server_url: URL) -> None:
    response = await http_client.crawl(Request.from_url(str(server_url)))
    http_response = response.http_response

    assert http_response.status_code == 200
    with pytest.raises(RuntimeError):
        [item async for item in http_response.read_stream()]


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


async def test_compressed_chunked_stream(http_client: HttpClient, server_url: URL) -> None:
    content_body: bytes = b''

    async with http_client.stream(str(server_url / 'get_compressed')) as response:
        assert response.status_code == 200
        async for chunk in response.read_stream():
            content_body += chunk

    assert content_body == HELLO_WORLD * 1000


async def test_send_request_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        await http_client.send_request('gopher://127.0.0.1:6379/_PING')


async def test_stream_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        async with http_client.stream('gopher://127.0.0.1:6379/_PING'):
            pass


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


async def test_stream_follows_redirects(http_client: HttpClient, server_url: URL) -> None:
    """Test that streamed requests follow redirects and carry session cookies along."""
    session = Session()
    stream_url = (server_url / 'set_cookies').with_query(a='1')

    async with http_client.stream(str(stream_url), session=session) as response:
        content = b''
        async for chunk in response.read_stream():
            content += chunk

    assert json.loads(content.decode())['cookies'] == {'a': '1'}
    assert {item['name'] for item in session.cookies.get_cookies_as_dicts()} == {'a'}


async def test_crawl_keeps_cookies_and_encoding(http_client: HttpClient, server_url: URL) -> None:
    """Test that `crawl` carries session cookies through a redirect and sends signed URLs without re-encoding."""
    session = Session(cookies=[CookieParam(name='preset', value='value', domain=server_url.host or '')])

    signed_query = 'X-Amz-Credential=AKIA%2F20240101%2Fus-east-1&X-Amz-Date=2024-01-01T00%3A00%3A00Z'
    target_url = f'{server_url / "cookies"}?{signed_query}'

    direct_request = Request.from_url(target_url)
    direct_result = await http_client.crawl(direct_request, session=session)

    assert (await read_json(direct_result.http_response))['cookies'] == {'preset': 'value'}
    assert direct_request.loaded_url == target_url

    redirected_request = Request.from_url(str((server_url / 'redirect').with_query(url=target_url, status=302)))
    redirected_result = await http_client.crawl(redirected_request, session=session)

    assert (await read_json(redirected_result.http_response))['cookies'] == {'preset': 'value'}
    assert redirected_request.loaded_url == target_url
