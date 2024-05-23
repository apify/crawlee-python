from __future__ import annotations

import pytest

from crawlee.browsers.browser_pool import BrowserPool
from crawlee.browsers.playwright_browser_plugin import PlaywrightBrowserPlugin

pytestmark = pytest.mark.only()


# @pytest.fixture()
# async def playwright() -> AsyncGenerator[Playwright, None]:
#     async with async_playwright() as playwright:
#         yield playwright


async def test_browser_pool_init() -> None:
    playwright_plugin = PlaywrightBrowserPlugin()
    browser_pool = BrowserPool([playwright_plugin])
    assert browser_pool.plugins == [playwright_plugin]


async def test_browser_pool_one_plugin() -> None:
    plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([plugin]) as browser_pool:
        assert browser_pool.plugins == [plugin]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'

        page_2 = await browser_pool.new_page()
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'chromium'

        await page_1.page.close()
        await page_2.page.close()


async def test_browser_pool_more_plugins() -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'

        page_2 = await browser_pool.new_page()
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'firefox'

        page_3 = await browser_pool.new_page()
        await page_3.page.goto('https://example.com/')
        assert page_3.browser_type == 'chromium'

        await page_1.page.close()
        await page_2.page.close()
        await page_3.page.close()


#
