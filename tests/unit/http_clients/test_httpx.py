from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.errors import ProxyError
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT_LANGUAGE
from crawlee.http_clients import HttpxHttpClient
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.proxy_configuration import ProxyInfo


@pytest.fixture
def http_client() -> HttpxHttpClient:
    return HttpxHttpClient(http2=False)


async def test_http_1(server_url: URL) -> None:
    http_client = HttpxHttpClient(http1=True, http2=False)
    response = await http_client.send_request(str(server_url))
    assert response.http_version == 'HTTP/1.1'


async def test_http_2() -> None:
    http_client = HttpxHttpClient(http2=True)
    response = await http_client.send_request('https://apify.com/')
    assert response.http_version == 'HTTP/2'


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy(
    http_client: HttpxHttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    url = str(server_url / 'status/222')
    request = Request.from_url(url)

    async with Statistics.with_default_state() as statistics:
        result = await http_client.crawl(request, proxy_info=proxy, statistics=statistics)

    assert result.http_response.status_code == 222


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy_disabled(
    http_client: HttpxHttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'
    request = Request.from_url(url)

    with pytest.raises(ProxyError):
        async with Statistics.with_default_state() as statistics:
            await http_client.crawl(request, proxy_info=disabled_proxy, statistics=statistics)


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy(
    http_client: HttpxHttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    url = str(server_url / 'status/222')

    response = await http_client.send_request(url, proxy_info=proxy)
    assert response.status_code == 222  # 222 - authentication successful


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy_disabled(
    http_client: HttpxHttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'

    with pytest.raises(ProxyError):
        await http_client.send_request(url, proxy_info=disabled_proxy)


async def test_common_headers_and_user_agent(server_url: URL, header_network: dict) -> None:
    client = HttpxHttpClient()

    response = await client.send_request(str(server_url / 'headers'))
    response_headers = json.loads(response.read().decode())

    assert 'accept' in response_headers
    assert response_headers['accept'] in get_available_header_values(header_network, {'Accept', 'accept'})

    assert 'accept-language' in response_headers
    assert response_headers['accept-language'] == COMMON_ACCEPT_LANGUAGE

    # By default, HTTPX uses its own User-Agent, which should be replaced by the one from the header generator.
    assert 'user-agent' in response_headers
    assert 'python-httpx' not in response_headers['user-agent']
    assert response_headers['user-agent'] in get_available_header_values(header_network, {'User-Agent', 'user-agent'})


async def test_crawl_follow_redirects_by_default(http_client: HttpxHttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)

    crawling_result = await http_client.crawl(request)

    assert crawling_result.http_response.status_code == 200
    assert request.loaded_url == target_url


async def test_crawl_follow_redirects_false(server_url: URL) -> None:
    http_client = HttpxHttpClient(follow_redirects=False, http2=False)

    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)

    crawling_result = await http_client.crawl(request)

    assert crawling_result.http_response.status_code == 302
    assert crawling_result.http_response.headers['Location'] == target_url
    assert request.loaded_url == redirect_url
