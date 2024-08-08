from __future__ import annotations

from pathlib import Path
from typing import AsyncGenerator

import pytest

from crawlee.browsers import PlaywrightBrowserPlugin


@pytest.fixture()
async def plugin(tmp_path: Path) -> AsyncGenerator[PlaywrightBrowserPlugin, None]:
    async with PlaywrightBrowserPlugin(browser_options={'user_data_dir': tmp_path / 'profile'}) as plugin:
        yield plugin


async def test_new_browser(plugin: PlaywrightBrowserPlugin, httpbin: str) -> None:
    browser_controller = await plugin.new_browser()

    # assert browser_controller.is_browser_connected

    page = await browser_controller.new_page()
    await page.goto(f'{httpbin}')

    await page.close()
    await browser_controller.close()

    # assert not browser_controller.is_browser_connected

    # check if profile directory does contain some files
    user_data_dir = plugin.browser_options.get('user_data_dir')
    assert user_data_dir is not None
    assert any(Path(user_data_dir).iterdir())
