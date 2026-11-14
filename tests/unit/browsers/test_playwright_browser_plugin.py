from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.browsers import PlaywrightBrowserPlugin


async def test_initial_state() -> None:
    plugin = PlaywrightBrowserPlugin(
        browser_type='chromium',
        browser_launch_options={'headless': False},
        browser_new_context_options={'viewport': {'width': 1920, 'height': 1080}},
        max_open_pages_per_browser=10,
    )

    # Test initial state
    assert plugin.browser_type == 'chromium'
    assert 'headless' in plugin.browser_launch_options
    assert plugin.browser_launch_options['headless'] is False
    assert plugin.browser_new_context_options == {'viewport': {'width': 1920, 'height': 1080}}
    assert plugin.max_open_pages_per_browser == 10


async def test_executable_path_with_chrome_browser_type() -> None:
    """An `executable_path` with `browser_type='chrome'` is kept, and the plugin runs Chromium on the Chrome channel."""
    plugin = PlaywrightBrowserPlugin(
        browser_type='chrome',
        browser_launch_options={'executable_path': '/path/to/chrome'},
    )

    assert plugin.browser_type == 'chromium'
    assert plugin.browser_launch_options['executable_path'] == '/path/to/chrome'
    assert plugin.browser_launch_options['channel'] == 'chrome'


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('playwright failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_enter_frees_plugin(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """A plugin whose entry fails or is cancelled stops Playwright, stays inactive and can be entered again."""
    plugin = PlaywrightBrowserPlugin()
    failing_context_manager = AsyncMock()
    failing_context_manager.__aenter__.side_effect = error
    monkeypatch.setattr(plugin, '_playwright_context_manager', failing_context_manager)
    monkeypatch.setattr('crawlee.browsers._playwright_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))

    with pytest.raises(type(error)):
        async with plugin:
            pass

    assert not plugin.active
    failing_context_manager.__aexit__.assert_awaited_once()

    async with plugin:
        assert plugin.active


async def test_failed_rollback_keeps_enter_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A Playwright that fails to stop during an entry rollback is logged and the entry error is kept."""
    plugin = PlaywrightBrowserPlugin()
    failing_context_manager = AsyncMock()
    failing_context_manager.__aenter__.side_effect = RuntimeError('start failed')
    failing_context_manager.__aexit__.side_effect = RuntimeError('stop failed')
    monkeypatch.setattr(plugin, '_playwright_context_manager', failing_context_manager)
    monkeypatch.setattr('crawlee.browsers._playwright_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))

    with pytest.raises(RuntimeError, match='start failed'):
        async with plugin:
            pass

    assert not plugin.active
    assert 'Stopping Playwright failed while rolling back the plugin entry.' in caplog.text


async def test_failed_exit_frees_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin whose exit fails is no longer active and can be entered again."""
    plugin = PlaywrightBrowserPlugin()
    failing_context_manager = AsyncMock()
    failing_context_manager.__aexit__.side_effect = RuntimeError('stop failed')
    monkeypatch.setattr(plugin, '_playwright_context_manager', failing_context_manager)
    monkeypatch.setattr('crawlee.browsers._playwright_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))

    with pytest.raises(RuntimeError, match='stop failed'):
        async with plugin:
            pass

    assert not plugin.active

    async with plugin:
        assert plugin.active


async def test_enter_during_exit_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin can't be entered again until its exit has stopped Playwright."""
    plugin = PlaywrightBrowserPlugin()
    stop_started = asyncio.Event()
    release_stop = asyncio.Event()

    async def wait_for_release(*_: object) -> None:
        stop_started.set()
        await release_stop.wait()

    playwright_context_manager = AsyncMock()
    playwright_context_manager.__aexit__.side_effect = wait_for_release
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._playwright_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))

    await plugin.__aenter__()
    exit_task = asyncio.create_task(plugin.__aexit__(None, None, None))
    await stop_started.wait()

    with pytest.raises(RuntimeError, match='already active'):
        await plugin.__aenter__()

    release_stop.set()
    await exit_task
