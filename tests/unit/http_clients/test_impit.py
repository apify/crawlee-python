from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.errors import ProxyError
from crawlee.http_clients import ImpitHttpClient
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from yarl import URL

    from crawlee.proxy_configuration import ProxyInfo


@pytest.fixture
def http_client() -> ImpitHttpClient:
    return ImpitHttpClient()


async def test_http_1(server_url: URL) -> None:
    http_client = ImpitHttpClient()
    response = await http_client.send_request(str(server_url))
    assert response.http_version == 'HTTP/1.1'


async def test_http_2() -> None:
    http_client = ImpitHttpClient()
    response = await http_client.send_request('https://apify.com/')
    assert response.http_version == 'HTTP/2'


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy(
    http_client: ImpitHttpClient,
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
    http_client: ImpitHttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'
    request = Request.from_url(url)

    with pytest.raises(ProxyError):
        async with Statistics.with_default_state() as statistics:
            await http_client.crawl(request, proxy_info=disabled_proxy, statistics=statistics)


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy(
    http_client: ImpitHttpClient,
    proxy: ProxyInfo,
    server_url: URL,
) -> None:
    url = str(server_url / 'status/222')

    response = await http_client.send_request(url, proxy_info=proxy)
    assert response.status_code == 222  # 222 - authentication successful


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_send_request_with_proxy_disabled(
    http_client: ImpitHttpClient,
    disabled_proxy: ProxyInfo,
) -> None:
    url = 'https://apify.com/'

    with pytest.raises(ProxyError):
        await http_client.send_request(url, proxy_info=disabled_proxy)


async def test_crawl_follow_redirects_by_default(http_client: ImpitHttpClient, server_url: URL) -> None:
    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)

    crawling_result = await http_client.crawl(request)

    assert crawling_result.http_response.status_code == 200
    assert request.loaded_url == target_url


async def test_crawl_follow_redirects_false(server_url: URL) -> None:
    http_client = ImpitHttpClient(follow_redirects=False)

    target_url = str(server_url / 'status/200')
    redirect_url = str((server_url / 'redirect').update_query(url=target_url))
    request = Request.from_url(redirect_url)

    crawling_result = await http_client.crawl(request)

    assert crawling_result.http_response.status_code == 302
    assert crawling_result.http_response.headers['Location'] == target_url
    assert request.loaded_url == redirect_url
