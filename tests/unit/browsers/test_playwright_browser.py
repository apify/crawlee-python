from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from playwright.async_api import async_playwright

from crawlee.browsers._playwright_browser import PlaywrightPersistentBrowser

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from playwright.async_api import Playwright


@pytest.fixture
async def playwright() -> AsyncGenerator[Playwright, None]:
    async with async_playwright() as playwright:
        yield playwright


async def test_init(playwright: Playwright) -> None:
    browser_type = playwright.chromium
    persist_browser = PlaywrightPersistentBrowser(browser_type, user_data_dir=None, browser_launch_options={})
    assert persist_browser._browser_type == browser_type
    assert persist_browser.browser_type == browser_type
    assert persist_browser._browser_launch_options == {}
    assert persist_browser._temp_dir is None
    assert persist_browser._user_data_dir is None
    assert persist_browser._is_connected is True
    assert persist_browser.is_connected() is True


async def test_delete_temp_folder_with_close_browser(playwright: Playwright) -> None:
    persist_browser = PlaywrightPersistentBrowser(
        playwright.chromium, user_data_dir=None, browser_launch_options={'headless': True}
    )
    await persist_browser.new_context()
    assert isinstance(persist_browser._temp_dir, Path)
    current_temp_dir = persist_browser._temp_dir
    assert current_temp_dir.exists()
    await persist_browser.close()
    assert not current_temp_dir.exists()
