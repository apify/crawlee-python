from __future__ import annotations

from crawlee import Request
from crawlee.proxy_configuration import ProxyConfiguration


async def test_rotates_proxies_uniformly_with_no_request() -> None:
    tiered_proxy_urls: list[list[str | None]] = [
        ['http://proxy:1111', 'http://proxy:2222'],
        ['http://proxy:3333', 'http://proxy:4444'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][1]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[1][0]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[1][1]

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]


async def test_retrying_request_makes_tier_go_up() -> None:
    tiered_proxy_urls: list[list[str | None]] = [
        ['http://proxy:1111'],
        ['http://proxy:2222'],
        ['http://proxy:3333'],
        ['http://proxy:4444'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    # Calling `new_proxy_info` with the same request most probably means it's being retried
    request_1 = Request(url='http://some.domain/abc', unique_key='1', id='1')

    info = await config.new_proxy_info(None, request_1, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]

    info = await config.new_proxy_info(None, request_1, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[1][0]

    info = await config.new_proxy_info(None, request_1, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[2][0]

    # Subsequent requests with the same domain should use the same tier
    request_2 = Request(url='http://some.domain/xyz', unique_key='2', id='2')

    info = await config.new_proxy_info(None, request_2, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[2][0]


async def test_successful_request_makes_tier_go_down() -> None:
    """Repeatedly requesting a proxy for a single request will cause the proxy tier to go up -
    ProxyConfiguration assumes those are retries. Then, requesting a proxy for different requests to the same domain
    will cause the tier to drop back down."""

    tiered_proxy_urls: list[list[str | None]] = [
        ['http://proxy:1111'],
        ['http://proxy:2222'],
        ['http://proxy:3333'],
        ['http://proxy:4444'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    request_1 = Request(url='http://some.domain/abc', unique_key='1', id='1')

    info = None
    for tier in tiered_proxy_urls:
        info = await config.new_proxy_info('session_id', request_1, None)
        assert info is not None
        assert info.url == tier[0]

    for i in range(100):
        new_request = Request(url=f'http://some.domain/{i}', unique_key=str(i), id=str(i))
        info = await config.new_proxy_info('session_id', new_request, None)

    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]


async def test_none_proxy_retrying_request_makes_tier_go_up() -> None:
    tiered_proxy_urls: list[list[str | None]] = [
        [None],
        ['http://proxy:1111'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    # Calling `new_proxy_info` with the same request most probably means it's being retried
    request_1 = Request(url='http://some.domain/abc', unique_key='1', id='1')

    # No proxy used.
    info = await config.new_proxy_info(None, request_1, None)
    assert info is None

    info = await config.new_proxy_info(None, request_1, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[1][0]


async def test_none_proxy_rotates_proxies_uniformly_with_no_request() -> None:
    tiered_proxy_urls = [
        [None, 'http://proxy:1111'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    # No proxy used.
    info = await config.new_proxy_info(None, None, None)
    assert info is None

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][1]

    # No proxy used.
    info = await config.new_proxy_info(None, None, None)
    assert info is None
