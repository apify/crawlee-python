from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from curl_cffi import CurlHttpVersion

from crawlee import Request
from crawlee.errors import ProxyError
from crawlee.http_clients import CurlImpersonateHttpClient, HttpClient, HttpxHttpClient, ImpitHttpClient
from crawlee.statistics import Statistics
from tests.unit.server_endpoints import HELLO_WORLD

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from _pytest.fixtures import SubRequest
    from yarl import URL

    from crawlee.proxy_configuration import ProxyInfo


@pytest.fixture
async def custom_http_client(request: SubRequest) -> AsyncGenerator[HttpClient]:
    """Helper fixture to reduce code duplication.

    If clients are not initialized, create their default instances.
    Return client in active context, leave the context after the test."""

    client = request.param if isinstance(request.param, HttpClient) else request.param()
    async with client as _:
        yield _


async def test_http_1(http_client: HttpClient, server_url: URL) -> None:
    response = await http_client.send_request(str(server_url))
    assert response.http_version == 'HTTP/1.1'


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
