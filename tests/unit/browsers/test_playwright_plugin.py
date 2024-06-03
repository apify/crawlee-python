from __future__ import annotations

import pytest

from crawlee.browsers.playwright_browser_plugin import PlaywrightBrowserPlugin


async def test_new_page(httpbin: str) -> None:
    async with PlaywrightBrowserPlugin() as plugin:
        # Get a new page with default options
        page_1 = await plugin.new_page()
        await page_1.goto(f'{httpbin}/get')
        assert page_1.url == f'{httpbin}/get'
        assert '<html' in await page_1.content()  # there is some HTML content

    page_options = {
        'viewport': {'width': 1920, 'height': 1080},
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0) Chrome/58.0 Safari/537.36',
        'java_script_enabled': False,
    }

    async with PlaywrightBrowserPlugin(page_options=page_options) as plugin:
        # Get a new page with custom options
        page_2 = await plugin.new_page()
        await page_2.goto(f'{httpbin}/user-agent')
        assert page_2.url == f'{httpbin}/user-agent'
        assert '<html' in await page_2.content()  # there is some HTML content


async def test_resource_management(httpbin: str) -> None:
    async with PlaywrightBrowserPlugin() as plugin:
        assert plugin.browser is not None
        # Browser should be connected
        assert plugin.browser.is_connected() is True

        page = await plugin.new_page()
        await page.goto(f'{httpbin}/get')
        assert page.url == f'{httpbin}/get'
        assert '<html' in await page.content()  # there is some HTML content

    # The page should be closed
    assert page.is_closed()
    # The browser should be disconnected
    assert plugin.browser.is_connected() is False


async def test_raises_error_when_not_initialized() -> None:
    plugin = PlaywrightBrowserPlugin()
    with pytest.raises(RuntimeError, match='Playwright browser plugin is not initialized.'):
        await plugin.new_page()
