from __future__ import annotations

import pytest

from crawlee.browsers.browser_pool import BrowserPool
from crawlee.browsers.playwright_browser_plugin import PlaywrightBrowserPlugin


async def test_new_page_single_plugin() -> None:
    plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([plugin]) as browser_pool:
        assert browser_pool.plugins == [plugin]

        page_1 = await browser_pool.new_page()
        assert page_1 is not None
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == 'https://apify.com/'

        page_2 = await browser_pool.new_page()
        assert page_2 is not None
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'chromium'
        assert page_2.page.url == 'https://crawlee.dev/'


async def test_new_page_multiple_plugins() -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.new_page()
        assert page_1 is not None
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == 'https://apify.com/'

        page_2 = await browser_pool.new_page()
        assert page_2 is not None
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'firefox'
        assert page_2.page.url == 'https://crawlee.dev/'

        page_3 = await browser_pool.new_page()
        assert page_3 is not None
        await page_3.page.goto('https://example.com/')
        assert page_3.browser_type == 'chromium'
        assert page_3.page.url == 'https://example.com/'


async def test_new_page_with_each_plugin() -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        pages = await browser_pool.new_page_with_each_plugin()

        assert len(pages) == 2
        assert pages[0].browser_type == 'chromium'
        assert pages[1].browser_type == 'firefox'

        await pages[0].page.goto('https://example.com/')
        await pages[1].page.goto('https://example.com/')

        # Ensure the pages are working by checking their titles or another element
        assert await pages[0].page.title() == 'Example Domain'
        assert await pages[1].page.title() == 'Example Domain'

        await pages[0].page.close()
        await pages[1].page.close()


async def test_resource_management() -> None:
    playwright_plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([playwright_plugin]) as browser_pool:
        page = await browser_pool.new_page()
        assert page is not None
        await page.page.goto('https://apify.com/')
        assert page.page.url == 'https://apify.com/'

    # The page should be closed
    assert page.page.is_closed()

    # Browsers in all plugins should be disconnected
    for plugin in browser_pool.plugins:
        assert plugin.browser is not None
        assert plugin.browser.is_connected() is False


async def test_raises_error_when_not_initialized() -> None:
    plugin = PlaywrightBrowserPlugin()
    browser_pool = BrowserPool([plugin])
    with pytest.raises(RuntimeError, match='Browser pool is not initialized.'):
        await browser_pool.new_page()
