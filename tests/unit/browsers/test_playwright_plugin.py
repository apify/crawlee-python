from __future__ import annotations

import pytest

from crawlee.browsers.playwright_browser_plugin import PlaywrightBrowserPlugin

pytestmark = pytest.mark.only()


async def test_new_page() -> None:
    async with PlaywrightBrowserPlugin() as plugin:
        # Get a new page with default options
        page_1 = await plugin.new_page()
        await page_1.goto('https://apify.com/')
        assert page_1.url == 'https://apify.com/'

        # Get a new page with custom options
        page_2_options = {
            'viewport': {'width': 1920, 'height': 1080},
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0) Chrome/58.0 Safari/537.36',
            'java_script_enabled': False,
        }
        page_2 = await plugin.new_page(page_options=page_2_options)
        await page_2.goto('https://crawlee.dev/')
        assert page_2.url == 'https://crawlee.dev/'


async def test_resource_management() -> None:
    async with PlaywrightBrowserPlugin() as plugin:
        # Browser should be connected
        assert plugin.browser.is_connected() is True

        page = await plugin.new_page()
        await page.goto('https://apify.com/')
        assert page.url == 'https://apify.com/'

    # The page should be closed
    assert page.is_closed()
    # The browser should be disconnected
    assert plugin.browser.is_connected() is False


async def test_raises_error_when_not_initialized() -> None:
    plugin = PlaywrightBrowserPlugin()
    with pytest.raises(RuntimeError, match='Playwright browser plugin is not initialized.'):
        await plugin.new_page()
