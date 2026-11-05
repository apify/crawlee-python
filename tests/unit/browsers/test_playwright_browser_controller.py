from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from playwright.async_api import Browser, BrowserContext, Page

from crawlee.browsers import PlaywrightBrowserController, PlaywrightPersistentBrowser
from crawlee.proxy_configuration import ProxyInfo


async def test_memory_leak_on_concurrent_context_creation() -> None:
    """Test that only one browser context is created when multiple pages are opened concurrently."""

    # Prepare mocked browser with relevant methods and attributes
    mocked_browser = AsyncMock()
    mocked_context_launcher = AsyncMock()
    mocked_context = AsyncMock(spec=BrowserContext)

    mocked_context_launcher.return_value = mocked_context
    mocked_context.new_page.return_value = AsyncMock(spec=Page)

    async def delayed_launch_persistent_context(*args: Any, **kwargs: Any) -> Any:
        """Ensure that both calls to create context overlap in time."""
        await asyncio.sleep(5)  # Simulate delay in creation to make sure race condition happens
        return await mocked_context_launcher(*args, **kwargs)

    mocked_browser.launch_persistent_context = delayed_launch_persistent_context

    # Create minimal instance of PlaywrightBrowserController with mocked browser
    controller = PlaywrightBrowserController(
        PlaywrightPersistentBrowser(mocked_browser, None, browser_launch_options={}),
        header_generator=None,
        fingerprint_generator=None,
    )

    # Both calls will try to create browser context at the same time, but only one context should be created.
    await asyncio.gather(controller.new_page(), controller.new_page())

    assert mocked_context_launcher.call_count == 1


@pytest.mark.parametrize(
    'use_incognito_pages',
    [
        pytest.param(True, id='incognito'),
        pytest.param(False, id='persistent'),
    ],
)
@pytest.mark.parametrize(
    ('ignore_tls_errors', 'context_options', 'expected'),
    [
        pytest.param(True, None, True, id='intercepting_proxy'),
        pytest.param(False, None, None, id='regular_proxy'),
        pytest.param(True, {'ignore_https_errors': False}, True, id='overrides_context_option'),
    ],
)
async def test_proxy_ignore_tls_errors_reaches_context(
    *,
    ignore_tls_errors: bool,
    context_options: dict[str, Any] | None,
    expected: bool | None,
    use_incognito_pages: bool,
) -> None:
    """Test that a proxy with `ignore_tls_errors` makes the browser context ignore TLS certificate errors."""
    browser_context = AsyncMock(spec=BrowserContext)
    browser_context.new_page.return_value = Mock(spec=Page)
    browser = AsyncMock(spec=Browser, contexts=[])
    browser.new_context.return_value = browser_context
    controller = PlaywrightBrowserController(browser, use_incognito_pages=use_incognito_pages, header_generator=None)
    proxy_info = ProxyInfo(
        url='http://proxy.example.com:8080',
        scheme='http',
        hostname='proxy.example.com',
        port=8080,
        ignore_tls_errors=ignore_tls_errors,
    )

    await controller.new_page(browser_new_context_options=context_options, proxy_info=proxy_info)

    assert browser.new_context.call_args.kwargs.get('ignore_https_errors') is expected
