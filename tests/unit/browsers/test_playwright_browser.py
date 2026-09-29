from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

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


async def test_delete_temp_folder_when_files_are_locked(monkeypatch: pytest.MonkeyPatch) -> None:
    """The temp directory is removed even when the first delete attempts fail, as Windows locks the browser files."""
    monkeypatch.setattr(PlaywrightPersistentBrowser, '_TMP_DIR_DELETE_INTERVAL', timedelta(0))

    real_rmtree = shutil.rmtree
    locked_attempts = 3
    rmtree = Mock()

    def rmtree_locked_at_first(path: Any, **kwargs: Any) -> None:
        """Model `rmtree(ignore_errors=True)` silently leaving the directory in place while a file is locked."""
        if rmtree.call_count > locked_attempts:
            real_rmtree(path, **kwargs)

    rmtree.side_effect = rmtree_locked_at_first
    monkeypatch.setattr(shutil, 'rmtree', rmtree)

    # A real browser on Windows can hold the files longer than the whole retry budget, so a fake context stands in.
    # Like Playwright, it runs the `close` listener as a separate task. Letting that task start first makes `close` wait
    # for the removal the listener is running.
    context = Mock()
    listener_tasks = list[asyncio.Task]()

    async def close_context() -> None:
        listener = context.on.call_args.args[1]
        listener_tasks.append(asyncio.create_task(listener(context)))
        await asyncio.sleep(0)

    context.close = close_context
    browser_type = Mock()
    browser_type.launch_persistent_context = AsyncMock(return_value=context)

    persist_browser = PlaywrightPersistentBrowser(browser_type, user_data_dir=None, browser_launch_options={})
    await persist_browser.new_context()
    assert isinstance(persist_browser._temp_dir, Path)
    current_temp_dir = persist_browser._temp_dir
    assert current_temp_dir.exists()
    await persist_browser.close()
    assert not current_temp_dir.exists()
    await asyncio.gather(*listener_tasks)
    # The context's `close` event and `close` itself both ask for the removal, but only one of them retries.
    assert rmtree.call_count == locked_attempts + 1


async def test_warn_when_temp_folder_cannot_be_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A temp directory that stays locked for the whole retry budget is reported with a warning."""
    monkeypatch.setattr(PlaywrightPersistentBrowser, '_TMP_DIR_DELETE_ATTEMPTS', 2)
    monkeypatch.setattr(PlaywrightPersistentBrowser, '_TMP_DIR_DELETE_INTERVAL', timedelta(0))
    monkeypatch.setattr(shutil, 'rmtree', Mock())

    persist_browser = PlaywrightPersistentBrowser(Mock(), user_data_dir=None, browser_launch_options={})
    persist_browser._temp_dir = tmp_path

    with caplog.at_level(logging.WARNING, logger='crawlee.browsers._playwright_browser'):
        await persist_browser._delete_temp_dir()

    assert 'Could not remove the temporary user data directory' in caplog.text
