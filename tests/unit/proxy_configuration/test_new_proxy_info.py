from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from httpx import InvalidURL

from crawlee.proxy_configuration import ProxyConfiguration

if TYPE_CHECKING:
    from crawlee import Request


async def test_returns_proxy_info() -> None:
    config = ProxyConfiguration(proxy_urls=['http://proxy.com:1111'])

    proxy_info = await config.new_proxy_info(None, None, None)

    assert proxy_info is not None
    assert proxy_info.url == 'http://proxy.com:1111'
    assert proxy_info.hostname == 'proxy.com'
    assert proxy_info.username == ''
    assert proxy_info.password == ''
    assert proxy_info.port == 1111


async def test_throws_on_invalid_new_url_function() -> None:
    config = ProxyConfiguration(
        new_url_function=lambda session_id=None, request=None: 'http://proxy.com:1111*invalid_url'  # noqa: ARG005
    )

    with pytest.raises(ValueError) as exc:  # noqa: PT011
        await config.new_proxy_info(None, None, None)

    assert isinstance(exc.value.__cause__, InvalidURL)


async def test_returns_proxy_info_with_new_url_function() -> None:
    config = ProxyConfiguration(new_url_function=lambda session_id=None, request=None: 'http://proxy.com:1111')  # noqa: ARG005

    proxy_info = await config.new_proxy_info(None, None, None)

    assert proxy_info is not None
    assert proxy_info.url == 'http://proxy.com:1111'
    assert proxy_info.hostname == 'proxy.com'
    assert proxy_info.username == ''
    assert proxy_info.password == ''
    assert proxy_info.port == 1111


async def test_returns_proxy_info_with_new_url_function_async() -> None:
    async def new_url(session_id: str | None = None, request: Request | None = None) -> str:  # noqa: ARG001
        return 'http://proxy.com:1111'

    config = ProxyConfiguration(new_url_function=new_url)

    proxy_info = await config.new_proxy_info(None, None, None)

    assert proxy_info is not None
    assert proxy_info.url == 'http://proxy.com:1111'
    assert proxy_info.hostname == 'proxy.com'
    assert proxy_info.username == ''
    assert proxy_info.password == ''
    assert proxy_info.port == 1111


async def test_rotates_proxies() -> None:
    proxy_urls = ['http://proxy:1111', 'http://proxy:2222', 'http://proxy:3333']
    config = ProxyConfiguration(proxy_urls=proxy_urls)

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == proxy_urls[0]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == proxy_urls[1]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == proxy_urls[2]


async def test_rotates_proxies_with_sessions() -> None:
    proxy_urls = ['http://proxy:1111', 'http://proxy:2222', 'http://proxy:3333']
    sessions = [f'session_{i}' for i in range(6)]

    config = ProxyConfiguration(proxy_urls=proxy_urls)

    # A single session should always receive the same proxy
    info = await config.new_proxy_info(sessions[0], None, None)
    assert info is not None
    assert info.url == proxy_urls[0]

    info = await config.new_proxy_info(sessions[0], None, None)
    assert info is not None
    assert info.url == proxy_urls[0]

    info = await config.new_proxy_info(sessions[0], None, None)
    assert info is not None
    assert info.url == proxy_urls[0]

    # Different sessions should get rotated proxies
    info = await config.new_proxy_info(sessions[1], None, None)
    assert info is not None
    assert info.url == proxy_urls[1]

    info = await config.new_proxy_info(sessions[2], None, None)
    assert info is not None
    assert info.url == proxy_urls[2]

    info = await config.new_proxy_info(sessions[3], None, None)
    assert info is not None
    assert info.url == proxy_urls[0]

    info = await config.new_proxy_info(sessions[4], None, None)
    assert info is not None
    assert info.url == proxy_urls[1]

    info = await config.new_proxy_info(sessions[5], None, None)
    assert info is not None
    assert info.url == proxy_urls[2]


@pytest.mark.parametrize(
    ('url', 'expected_port'),
    [
        # Default ports based on the URL scheme
        ('http://proxy.com', 80),
        ('https://proxy.com', 443),
        # Explicit ports specified in the URL
        ('http://proxy.com:80', 80),
        ('http://proxy.com:1234', 1234),
    ],
)
async def test_sets_port(url: str, expected_port: int) -> None:
    """Test that the port property is set correctly.

    The port is inferred from the URL scheme if it is not specified in the URL.
    """
    config = ProxyConfiguration(proxy_urls=[url])

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.port == expected_port
