from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin

if TYPE_CHECKING:
    from yarl import URL


async def test_default_plugin_new_page_creation(server_url: URL) -> None:
    async with BrowserPool() as browser_pool:
        page_1 = await browser_pool.new_page()
        await page_1.page.goto(str(server_url))
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == str(server_url)
        assert '<html' in await page_1.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 1

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(str(server_url / 'status/200'))
        assert page_2.browser_type == 'chromium'
        assert page_2.page.url == str(server_url / 'status/200')
        assert '<html' in await page_1.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 2

        await page_1.page.close()
        await page_2.page.close()


async def test_multiple_plugins_new_page_creation(server_url: URL) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        assert browser_pool.plugins == [plugin_chromium, plugin_firefox]

        page_1 = await browser_pool.new_page()
        await page_1.page.goto(str(server_url))
        assert page_1.browser_type == 'chromium'
        assert page_1.page.url == str(server_url)
        assert '<html' in await page_1.page.content()  # there is some HTML content

        page_2 = await browser_pool.new_page()
        await page_2.page.goto(str(server_url / 'headers'))
        assert page_2.browser_type == 'firefox'
        assert page_2.page.url == str(server_url / 'headers')
        assert '<html' in await page_2.page.content()  # there is some HTML content

        page_3 = await browser_pool.new_page()
        await page_3.page.goto(str(server_url / 'user-agent'))
        assert page_3.browser_type == 'chromium'
        assert page_3.page.url == str(server_url / 'user-agent')
        assert '<html' in await page_3.page.content()  # there is some HTML content

        await page_1.page.close()
        await page_2.page.close()
        await page_3.page.close()

        assert browser_pool.total_pages_count == 3


async def test_new_page_with_each_plugin(server_url: URL) -> None:
    plugin_chromium = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_firefox = PlaywrightBrowserPlugin(browser_type='firefox')

    async with BrowserPool([plugin_chromium, plugin_firefox]) as browser_pool:
        pages = await browser_pool.new_page_with_each_plugin()

        assert len(pages) == 2

        assert pages[0].browser_type == 'chromium'
        assert pages[1].browser_type == 'firefox'

        await pages[0].page.goto(str(server_url))
        assert pages[0].page.url == str(server_url)
        assert '<html' in await pages[0].page.content()  # there is some HTML content

        await pages[1].page.goto(str(server_url / 'headers'))
        assert pages[1].page.url == str(server_url / 'headers')
        assert '<html' in await pages[1].page.content()

        for page in pages:
            await page.page.close()

        assert browser_pool.total_pages_count == 2


async def test_with_default_plugin_constructor(server_url: URL) -> None:
    async with BrowserPool.with_default_plugin(headless=True, browser_type='firefox') as browser_pool:
        assert len(browser_pool.plugins) == 1
        assert isinstance(browser_pool.plugins[0], PlaywrightBrowserPlugin)

        page = await browser_pool.new_page()
        assert page.browser_type == 'firefox'

        await page.page.goto(str(server_url))
        assert page.page.url == str(server_url)
        assert '<html' in await page.page.content()  # there is some HTML content

        await page.page.close()
        assert browser_pool.total_pages_count == 1


async def test_new_page_with_existing_id() -> None:
    async with BrowserPool() as browser_pool:
        page_1 = await browser_pool.new_page()
        with pytest.raises(ValueError, match='Page with ID: .* already exists.'):
            await browser_pool.new_page(page_id=page_1.id)


async def test_new_page_with_invalid_plugin() -> None:
    plugin_1 = PlaywrightBrowserPlugin(browser_type='chromium')
    plugin_2 = PlaywrightBrowserPlugin(browser_type='firefox')
    async with BrowserPool([plugin_1]) as browser_pool:
        with pytest.raises(ValueError, match='Provided browser_plugin is not one of the plugins used by BrowserPool.'):
            await browser_pool.new_page(browser_plugin=plugin_2)


async def test_resource_management(server_url: URL) -> None:
    playwright_plugin = PlaywrightBrowserPlugin(browser_type='chromium')

    async with BrowserPool([playwright_plugin]) as browser_pool:
        page = await browser_pool.new_page()
        await page.page.goto(str(server_url))
        assert page.page.url == str(server_url)
        assert '<html' in await page.page.content()  # there is some HTML content
        assert browser_pool.total_pages_count == 1

    # All pages should be closed in __aexit__
    assert page.page.is_closed()


async def test_methods_raise_error_when_not_active() -> None:
    plugin = PlaywrightBrowserPlugin()
    browser_pool = BrowserPool([plugin])

    assert browser_pool.active is False

    with pytest.raises(RuntimeError, match='BrowserPool is not active.'):
        await browser_pool.new_page()

    with pytest.raises(RuntimeError, match='BrowserPool is not active.'):
        await browser_pool.new_page_with_each_plugin()

    with pytest.raises(RuntimeError, match='BrowserPool is already active.'):
        async with browser_pool, browser_pool:
            pass

    async with browser_pool:
        assert browser_pool.active is True


async def test_with_plugin_contains_page_options(server_url: URL) -> None:
    plugin = PlaywrightBrowserPlugin(browser_new_context_options={'user_agent': 'My Best User-Agent'})
    async with BrowserPool(plugins=[plugin]) as browser_pool:
        test_page = await browser_pool.new_page()
        await test_page.page.goto(str(server_url / 'user-agent'))
        assert 'My Best User-Agent' in await test_page.page.content()
        await test_page.page.close()
