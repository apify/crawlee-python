from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.errors import ProxyError
from crawlee.fingerprint_suite._consts import COMMON_ACCEPT, COMMON_ACCEPT_LANGUAGE, USER_AGENT_POOL
from crawlee.http_clients import HttpxHttpClient
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.proxy_configuration import ProxyInfo


@pytest.fixture
def http_client() -> HttpxHttpClient:
    return HttpxHttpClient()


async def test_http_1(httpbin: URL) -> None:
    http_client = HttpxHttpClient(http1=True, http2=False)
    response = await http_client.send_request(str(httpbin))
    assert response.http_version == 'HTTP/1.1'


async def test_http_2(httpbin: URL) -> None:
    http_client = HttpxHttpClient(http2=True)
    response = await http_client.send_request(str(httpbin))
    assert response.http_version == 'HTTP/2'


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy(
    http_client: HttpxHttpClient,
    proxy: ProxyInfo,
    httpbin: URL,
) -> None:
    url = str(httpbin / 'status/222')
    request = Request.from_url(url)

    async with Statistics.with_default_state() as statistics:
        result = await http_client.crawl(request, proxy_info=proxy, statistics=statistics)

    assert result.http_response.status_code == 222


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy_disabled(
    http_client: HttpxHttpClient,
    disabled_proxy: ProxyInfo,
    httpbin: URL,
) -> None:
    url = str(httpbin / 'status/222')
    request = Request.from_url(url)

    with pytest.raises(ProxyError):
        async with Statistics.with_default_state() as statistics:
            await http_client.crawl(request, proxy_info=disabled_proxy, statistics=statistics)


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy(
    http_client: HttpxHttpClient,
    proxy: ProxyInfo,
    httpbin: URL,
) -> None:
    url = str(httpbin / 'status/222')

    response = await http_client.send_request(url, proxy_info=proxy)
    assert response.status_code == 222  # 222 - authentication successful


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy_disabled(
    http_client: HttpxHttpClient,
    disabled_proxy: ProxyInfo,
    httpbin: URL,
) -> None:
    url = str(httpbin / 'status/222')

    with pytest.raises(ProxyError):
        await http_client.send_request(url, proxy_info=disabled_proxy)


async def test_common_headers_and_user_agent(httpbin: URL) -> None:
    client = HttpxHttpClient()

    response = await client.send_request(str(httpbin / 'get'))
    response_dict = json.loads(response.read().decode())
    response_headers = response_dict.get('headers', {})

    assert 'Accept' in response_headers
    assert response_headers['Accept'] == COMMON_ACCEPT

    assert 'Accept-Language' in response_headers
    assert response_headers['Accept-Language'] == COMMON_ACCEPT_LANGUAGE

    # By default, HTTPX uses its own User-Agent, which should be replaced by the one from the header generator.
    assert 'User-Agent' in response_headers
    assert 'python-httpx' not in response_headers['User-Agent']
    assert response_headers['User-Agent'] in USER_AGENT_POOL
