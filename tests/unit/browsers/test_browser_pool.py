from __future__ import annotations

import pytest

from crawlee.browsers.browser_pool import BrowserPool
from crawlee.browsers.playwright_browser_plugin import PlaywrightBrowserPlugin


async def test_new_page_single_plugin(httpbin: str) -> None:
    plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([plugin]) as browser_pool:
        assert browser_pool.plugins == [plugin]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto(f'{httpbin}/get')
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == f'{httpbin}/get'
        assert '<html' in await page_1.page.content()  # there is some HTML content

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(f'{httpbin}/status/200')
        assert page_2.browser_type == 'chromium'
        assert page_2.page.url == f'{httpbin}/status/200'
        assert '<html' in await page_1.page.content()  # there is some HTML content


async def test_new_page_multiple_plugins(httpbin: str) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto(f'{httpbin}/get')
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == f'{httpbin}/get'
        assert '<html' in await page_1.page.content()  # there is some HTML content

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(f'{httpbin}/headers')
        assert page_2.browser_type == 'firefox'
        assert page_2.page.url == f'{httpbin}/headers'
        assert '<html' in await page_2.page.content()  # there is some HTML content

        page_3 = await browser_pool.new_page()
        await page_3.page.goto(f'{httpbin}/user-agent')
        assert page_3.browser_type == 'chromium'
        assert page_3.page.url == f'{httpbin}/user-agent'
        assert '<html' in await page_3.page.content()  # there is some HTML content


async def test_new_page_with_each_plugin(httpbin: str) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        pages = await browser_pool.new_page_with_each_plugin()

        assert len(pages) == 2

        assert pages[0].browser_type == 'chromium'
        assert pages[1].browser_type == 'firefox'

        await pages[0].page.goto(f'{httpbin}/get')
        assert pages[0].page.url == f'{httpbin}/get'
        assert '<html' in await pages[0].page.content()  # there is some HTML content

        await pages[1].page.goto(f'{httpbin}/headers')
        assert pages[1].page.url == f'{httpbin}/headers'
        assert '<html' in await pages[1].page.content()


async def test_resource_management(httpbin: str) -> None:
    playwright_plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([playwright_plugin]) as browser_pool:
        page = await browser_pool.new_page()
        await page.page.goto(f'{httpbin}/get')
        assert page.page.url == f'{httpbin}/get'
        assert '<html' in await page.page.content()  # there is some HTML content

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
