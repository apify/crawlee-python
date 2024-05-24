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

        page_1 = await browser_pool.get_new_page()
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'

        page_2 = await browser_pool.get_new_page()
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'chromium'

        await page_1.page.close()
        await page_2.page.close()


async def test_browser_pool_more_plugins() -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.get_new_page()
        await page_1.page.goto('https://apify.com/')
        assert page_1.browser_type == 'chromium'

        page_2 = await browser_pool.get_new_page()
        await page_2.page.goto('https://crawlee.dev/')
        assert page_2.browser_type == 'firefox'

        page_3 = await browser_pool.get_new_page()
        await page_3.page.goto('https://example.com/')
        assert page_3.browser_type == 'chromium'

        await page_1.page.close()
        await page_2.page.close()
        await page_3.page.close()


async def test_new_page_with_each_plugin() -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')
    # plugin_webkit = PlaywrightBrowserPlugin(browser_type='webkit')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        pages = await browser_pool.get_new_page_with_each_plugin()

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


#
