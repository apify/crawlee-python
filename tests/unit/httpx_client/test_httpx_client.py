import os
from collections.abc import AsyncGenerator

import pytest
from proxy import Proxy

from crawlee.basic_crawler.errors import ProxyError
from crawlee.http_clients import HttpxClient
from crawlee.models import Request
from crawlee.proxy_configuration import ProxyInfo
from crawlee.statistics import Statistics


@pytest.fixture()
async def proxy_info(unused_tcp_port: int) -> ProxyInfo:
    username = 'user'
    password = 'pass'

    return ProxyInfo(
        url=f'http://{username}:{password}@127.0.0.1:{unused_tcp_port}',
        scheme='http',
        hostname='127.0.0.1',
        port=unused_tcp_port,
        username=username,
        password=password,
    )


@pytest.fixture()
async def proxy(proxy_info: ProxyInfo) -> AsyncGenerator[ProxyInfo, None]:
    with Proxy(
        [
            '--hostname',
            proxy_info.hostname,
            '--port',
            str(proxy_info.port),
            '--basic-auth',
            f'{proxy_info.username}:{proxy_info.password}',
        ]
    ):
        yield proxy_info


@pytest.fixture()
async def disabled_proxy(proxy_info: ProxyInfo) -> AsyncGenerator[ProxyInfo, None]:
    with Proxy(
        [
            '--hostname',
            proxy_info.hostname,
            '--port',
            str(proxy_info.port),
            '--basic-auth',
            f'{proxy_info.username}:{proxy_info.password}',
            '--disable-http-proxy',
        ]
    ):
        yield proxy_info


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy(proxy: ProxyInfo, httpbin: str) -> None:
    client = HttpxClient()
    request = Request(url=f'{httpbin}/status/222', unique_key='42', id='42', user_data={})

    async with Statistics() as statistics:
        result = await client.crawl(request, None, proxy, statistics)

    assert result.http_response.status_code == 222


@pytest.mark.skipif(os.name == 'nt', reason='Skipped on Windows')
async def test_proxy_disabled(disabled_proxy: ProxyInfo, httpbin: str) -> None:
    client = HttpxClient()
    request = Request(url=f'{httpbin}/status/222', unique_key='42', id='42', user_data={})

    with pytest.raises(ProxyError):
        async with Statistics() as statistics:
            await client.crawl(request, None, disabled_proxy, statistics)
