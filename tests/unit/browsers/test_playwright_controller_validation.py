from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from playwright.async_api import Browser, Playwright, async_playwright

from crawlee.browsers import PlaywrightBrowserController

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def playwright() -> AsyncGenerator[Playwright, None]:
    async with async_playwright() as playwright:
        yield playwright


@pytest.fixture
async def browser(playwright: Playwright) -> AsyncGenerator[Browser, None]:
    browser = await playwright.chromium.launch()
    yield browser
    await browser.close()


async def test_controller_validation_typo_passed_through(browser: Browser) -> None:
    """Invalid options (e.g. typos) are passed through so Playwright raises its own error."""
    controller = PlaywrightBrowserController(browser)
    with pytest.raises(TypeError):
        await controller.new_page(browser_new_context_options={'headles': True})
    await controller.close()


async def test_controller_validation_cross_mode_persistent(browser: Browser, caplog: pytest.LogCaptureFixture) -> None:
    # Default is persistent mode (use_incognito_pages=False)
    controller = PlaywrightBrowserController(browser, use_incognito_pages=False)
    # storage_state is incognito-only
    with caplog.at_level(logging.WARNING):
        page = await controller.new_page(browser_new_context_options={'storage_state': {'cookies': [], 'origins': []}})
        assert 'Option "storage_state" is only supported in incognito context mode' in caplog.text
        await page.close()
    await controller.close()


async def test_controller_validation_cross_mode_incognito(browser: Browser, caplog: pytest.LogCaptureFixture) -> None:
    controller = PlaywrightBrowserController(browser, use_incognito_pages=True)
    # env is persistent-only
    with caplog.at_level(logging.WARNING):
        page = await controller.new_page(browser_new_context_options={'env': {}})
        assert 'Option "env" is only supported in persistent context mode' in caplog.text
        await page.close()
    await controller.close()


async def test_controller_validation_valid_common(browser: Browser) -> None:
    controller = PlaywrightBrowserController(browser)
    # viewport is common
    page = await controller.new_page(browser_new_context_options={'viewport': {'width': 800, 'height': 600}})
    assert page.viewport_size == {'width': 800, 'height': 600}
    await page.close()
    await controller.close()
