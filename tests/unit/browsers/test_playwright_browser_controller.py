from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from crawlee.browsers import PlaywrightBrowserController, PlaywrightPersistentBrowser

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from yarl import URL


@pytest.fixture
async def playwright() -> AsyncGenerator[Playwright, None]:
    async with async_playwright() as playwright:
        yield playwright


@pytest.fixture
async def browser(playwright: Playwright) -> AsyncGenerator[Browser, None]:
    browser = await playwright.chromium.launch()
    yield browser
    await browser.close()


@pytest.fixture
async def controller(browser: Browser) -> AsyncGenerator[PlaywrightBrowserController, None]:
    controller = PlaywrightBrowserController(browser, max_open_pages_per_browser=2)
    yield controller
    await controller.close()


async def test_initial_state(browser: Browser) -> None:
    controller = PlaywrightBrowserController(browser)

    # Test initial state
    assert controller.pages == []
    assert controller.pages_count == 0
    assert isinstance(controller.last_page_opened_at, datetime)
    assert controller.idle_time < timedelta(seconds=1)
    assert controller.has_free_capacity


async def test_open_and_close_page(controller: PlaywrightBrowserController, server_url: URL) -> None:
    page = await controller.new_page()
    await page.goto(str(server_url))

    assert page in controller.pages
    assert controller.pages_count == 1
    assert controller.last_page_opened_at <= datetime.now(timezone.utc)

    await page.close()

    assert page not in controller.pages
    assert controller.pages_count == 0


async def test_max_open_pages_limit(controller: PlaywrightBrowserController) -> None:
    page1 = await controller.new_page()
    assert controller.pages_count == 1

    page2 = await controller.new_page()
    assert controller.pages_count == 2

    with pytest.raises(ValueError, match=r'Cannot open more pages in this browser.'):
        await controller.new_page()

    assert controller.pages_count == 2

    await page1.close()
    assert controller.pages_count == 1

    page3 = await controller.new_page()
    assert controller.pages_count == 2

    await page2.close()
    await page3.close()

    assert controller.pages == []
    assert controller.pages_count == 0


async def test_idle_time(controller: PlaywrightBrowserController) -> None:
    idle_time_before = controller.idle_time
    await asyncio.sleep(1)  # Simulate waiting
    idle_time_after = controller.idle_time
    assert idle_time_after > idle_time_before


async def test_close_browser_with_open_pages(browser: Browser) -> None:
    controller = PlaywrightBrowserController(browser, max_open_pages_per_browser=2)
    _ = await controller.new_page()

    with pytest.raises(ValueError, match=r'Cannot close the browser while there are open pages.'):
        await controller.close()

    assert controller.pages_count == 1
    assert controller.is_browser_connected

    await controller.close(force=True)

    assert controller.pages_count == 0
    assert not controller.is_browser_connected


async def test_memory_leak_on_concurrent_context_creation() -> None:
    """Test that only one browser context is created when multiple pages are opened concurrently."""

    # Prepare mocked browser with relevant methods and attributes
    mocked_browser = AsyncMock()
    mocked_context_launcher = AsyncMock()
    mocked_context = AsyncMock(spec=BrowserContext)

    mocked_context_launcher.return_value = mocked_context
    mocked_context.new_page.return_value = AsyncMock(spec=Page)

    async def delayed_launch_persistent_context(*args: Any, **kwargs: Any) -> Any:
        """Ensure that both calls to create context overlap in time."""
        await asyncio.sleep(5)  # Simulate delay in creation to make sure race condition happens
        return await mocked_context_launcher(*args, **kwargs)

    mocked_browser.launch_persistent_context = delayed_launch_persistent_context

    # Create minimal instance of PlaywrightBrowserController with mocked browser
    controller = PlaywrightBrowserController(
        PlaywrightPersistentBrowser(mocked_browser, None, {}), header_generator=None, fingerprint_generator=None
    )

    # Both calls will try to create browser context at the same time, but only one context should be created.
    await asyncio.gather(controller.new_page(), controller.new_page())

    assert mocked_context_launcher.call_count == 1
