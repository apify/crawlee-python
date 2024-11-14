from __future__ import annotations

from typing import AsyncGenerator

import pytest

from crawlee.browsers import PlaywrightBrowserPlugin


@pytest.fixture
async def plugin() -> AsyncGenerator[PlaywrightBrowserPlugin, None]:
    async with PlaywrightBrowserPlugin() as plugin:
        yield plugin


async def test_initial_state() -> None:
    plugin = PlaywrightBrowserPlugin(
        browser_type='chromium',
        browser_options={'headless': False},
        page_options={'viewport': {'width': 1920, 'height': 1080}},
        max_open_pages_per_browser=10,
    )

    # Test initial state
    assert plugin.browser_type == 'chromium'
    assert plugin.browser_options == {'headless': False}
    assert plugin.page_options == {'viewport': {'width': 1920, 'height': 1080}}
    assert plugin.max_open_pages_per_browser == 10


async def test_new_browser(plugin: PlaywrightBrowserPlugin, httpbin: str) -> None:
    browser_controller = await plugin.new_browser()

    assert browser_controller.is_browser_connected

    page = await browser_controller.new_page()
    await page.goto(f'{httpbin}')

    await page.close()
    await browser_controller.close()

    assert not browser_controller.is_browser_connected


async def test_multiple_new_browsers(plugin: PlaywrightBrowserPlugin) -> None:
    browser_controller_1 = await plugin.new_browser()
    browser_controller_2 = await plugin.new_browser()

    assert browser_controller_1 is not browser_controller_2


async def test_new_browser_without_initialization() -> None:
    plugin = PlaywrightBrowserPlugin()
    with pytest.raises(RuntimeError):
        await plugin.new_browser()
