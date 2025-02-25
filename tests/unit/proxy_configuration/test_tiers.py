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


async def test_retrying_request_makes_tier_go_up_with_sessions() -> None:
    tiered_proxy_urls: list[list[str | None]] = [
        ['http://proxy:1111'],
        ['http://proxy:2222'],
        ['http://proxy:3333'],
        ['http://proxy:4444'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    request = Request(url='http://some.domain/abc', unique_key='1', id='1')

    # Calling `new_proxy_info` with the same request likely means that it is being retried.
    # However, a single session should always receive the same proxy
    info = await config.new_proxy_info('session_id', request, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]

    info = await config.new_proxy_info('session_id', request, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]

    info = await config.new_proxy_info('session_id', request, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[0][0]

    # For a new session, we will get a proxy from the corresponding tier
    info = await config.new_proxy_info('session_id2', request, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[3][0]

    info = await config.new_proxy_info('session_id2', request, None)
    assert info is not None
    assert info.url == tiered_proxy_urls[3][0]


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
        info = await config.new_proxy_info(None, request_1, None)
        assert info is not None
        assert info.url == tier[0]

    for i in range(100):
        new_request = Request(url=f'http://some.domain/{i}', unique_key=str(i), id=str(i))
        info = await config.new_proxy_info(None, new_request, None)

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
    assert info is None, 'First entry in tired_proxy_urls is None. config.new_proxy_info is expected to generate None.'

    # Proxy should go up one tier for same request that was already sent before.
    info = await config.new_proxy_info(None, request_1, None)
    assert info is not None, (
        'config.new_proxy_info is expected to generate non-none proxy info from non-none tiered_proxy_urls.'
    )
    assert info.url == tiered_proxy_urls[1][0]


async def test_none_proxy_rotates_proxies_uniformly_with_no_request() -> None:
    tiered_proxy_urls = [
        [None, 'http://proxy:1111'],
    ]

    config = ProxyConfiguration(tiered_proxy_urls=tiered_proxy_urls)

    # No proxy used.
    info = await config.new_proxy_info(None, None, None)
    assert info is None, 'First entry in tired_proxy_urls is None. config.new_proxy_info is expected to generate None.'

    # Proxy should be rotated on the same proxy tier for a new request.
    info = await config.new_proxy_info(None, None, None)
    assert info is not None, (
        'config.new_proxy_info is expected to generate non-none proxy info from non-none tiered_proxy_urls.'
    )
    assert info.url == tiered_proxy_urls[0][1]

    # Proxy rotation starts from the beginning of the proxy list after last proxy in tier was used. No proxy used again.
    info = await config.new_proxy_info(None, None, None)
    assert info is None, 'First entry in tired_proxy_urls is None. config.new_proxy_info is expected to generate None.'
