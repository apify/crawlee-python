from __future__ import annotations

from itertools import cycle
from typing import TYPE_CHECKING

from crawlee.proxy_configuration import ProxyConfiguration

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from crawlee import Request


async def test_new_url_function() -> None:
    """Test that new_url_function can return string and None."""
    dummy_proxy = 'http://proxy:1111'
    proxy_iterator = cycle([None, dummy_proxy])

    def custom_new_url_function(session_id: str | None = None, request: Request | None = None) -> str | None:  # noqa: ARG001
        return next(proxy_iterator)

    config = ProxyConfiguration(new_url_function=custom_new_url_function)

    info = await config.new_proxy_info(None, None, None)
    assert info is None

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == dummy_proxy


async def test_new_url_coroutine() -> None:
    """Test that new_url_function can return coroutine."""
    dummy_proxy = 'http://proxy:1111'
    proxy_iterator = cycle([None, dummy_proxy])

    async def custom_new_url_coroutine() -> str | None:
        return next(proxy_iterator)

    def custom_new_url_function(session_id: str | None = None, request: Request | None = None) -> Awaitable[str | None]:  # noqa: ARG001
        return custom_new_url_coroutine()

    config = ProxyConfiguration(new_url_function=custom_new_url_function)

    info = await config.new_proxy_info(None, None, None)
    assert info is None

    info = await config.new_proxy_info(None, None, None)
    assert info is not None
    assert info.url == dummy_proxy
