from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

from playwright.async_api import BrowserContext, Page

from crawlee.browsers import PlaywrightBrowserController, PlaywrightPersistentBrowser


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
