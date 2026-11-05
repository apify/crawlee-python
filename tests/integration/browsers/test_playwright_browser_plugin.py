from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee.browsers import PlaywrightBrowserPlugin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL


@pytest.fixture
async def plugin() -> AsyncGenerator[PlaywrightBrowserPlugin, None]:
    async with PlaywrightBrowserPlugin() as plugin:
        yield plugin


async def test_new_browser(plugin: PlaywrightBrowserPlugin, server_url: URL) -> None:
    browser_controller = await plugin.new_browser()

    assert browser_controller.is_browser_connected

    page = await browser_controller.new_page()
    await page.goto(str(server_url), timeout=60_000)

    await page.close()
    await browser_controller.close()

    assert not browser_controller.is_browser_connected


async def test_multiple_new_browsers(plugin: PlaywrightBrowserPlugin) -> None:
    browser_controller_1 = await plugin.new_browser()
    browser_controller_2 = await plugin.new_browser()

    assert browser_controller_1 is not browser_controller_2


async def test_methods_raise_error_when_not_active() -> None:
    plugin = PlaywrightBrowserPlugin()

    assert plugin.active is False

    with pytest.raises(RuntimeError, match=r'Plugin is not active'):
        await plugin.new_browser()

    with pytest.raises(RuntimeError, match=r'Plugin is already active.'):
        async with plugin, plugin:
            pass

    async with plugin:
        assert plugin.active is True
