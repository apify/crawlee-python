from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.browsers import StagehandBrowserPlugin, StagehandOptions


def test_initial_state() -> None:
    plugin = StagehandBrowserPlugin(max_open_pages_per_browser=5)

    assert plugin.active is False
    assert plugin.browser_type == 'chromium'
    assert plugin.max_open_pages_per_browser == 5

    # headless should be True by default
    assert plugin.browser_launch_options['headless'] is True


def test_implicit_set_options() -> None:
    plugin = StagehandBrowserPlugin(
        browser_new_context_options={'viewport': {'width': 1280, 'height': 720}},
        browser_launch_options={'headless': False},
        user_data_dir='./test',
    )

    assert plugin.browser_launch_options['headless'] is False
    assert plugin.browser_launch_options['viewport'] == {'width': 1280, 'height': 720}
    assert plugin.browser_launch_options['user_data_dir'] == './test'


def test_order_priority_of_implicit_options() -> None:
    # `browser_launch_options` takes priority over `browser_new_context_options` for shared keys,
    # while non-conflicting keys from both dicts are merged.
    plugin = StagehandBrowserPlugin(
        browser_new_context_options={'headless': True, 'viewport': {'width': 1280, 'height': 720}},
        browser_launch_options={'headless': False},
    )

    assert plugin.browser_launch_options['headless'] is False
    assert plugin.browser_launch_options['viewport'] == {'width': 1280, 'height': 720}


def test_no_sandbox_added_when_sandbox_disabled() -> None:
    # The autouse fixture in conftest sets CRAWLEE_DISABLE_BROWSER_SANDBOX=true.
    plugin = StagehandBrowserPlugin()

    assert plugin.browser_launch_options['chromium_sandbox'] is False
    assert '--no-sandbox' in plugin.browser_launch_options['args']


def test_no_sandbox_not_added_when_sandbox_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('CRAWLEE_DISABLE_BROWSER_SANDBOX', 'false')
    plugin = StagehandBrowserPlugin()

    assert plugin.browser_launch_options['chromium_sandbox'] is True
    assert 'args' not in plugin.browser_launch_options


def test_no_sandbox_not_duplicated_when_already_in_args() -> None:
    plugin = StagehandBrowserPlugin(browser_launch_options={'args': ['--no-sandbox', '--disable-gpu']})

    assert plugin.browser_launch_options['args'].count('--no-sandbox') == 1
    assert '--disable-gpu' in plugin.browser_launch_options['args']


def test_stagehand_options_defaults_when_not_provided() -> None:
    plugin = StagehandBrowserPlugin()

    assert isinstance(plugin.stagehand_options, StagehandOptions)
    assert plugin.stagehand_options == StagehandOptions()


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('stagehand failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_enter_stops_playwright(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """A plugin whose Stagehand client fails to start or is cancelled stops Playwright and can be entered again."""
    plugin = StagehandBrowserPlugin()
    playwright_context_manager = AsyncMock()
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    stagehand_client = AsyncMock()
    stagehand_client.__aenter__.side_effect = error
    monkeypatch.setattr(
        'crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=stagehand_client)
    )

    with pytest.raises(type(error)):
        async with plugin:
            pass

    assert not plugin.active
    playwright_context_manager.__aexit__.assert_awaited_once()

    stagehand_client.__aenter__.side_effect = None
    async with plugin:
        assert plugin.active


async def test_failed_exit_stops_playwright(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin whose Stagehand client fails to close still stops Playwright and can be entered again."""
    plugin = StagehandBrowserPlugin()
    playwright_context_manager = AsyncMock()
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    stagehand_client = AsyncMock()
    stagehand_client.__aexit__.side_effect = RuntimeError('close failed')
    monkeypatch.setattr(
        'crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=stagehand_client)
    )

    with pytest.raises(RuntimeError, match='close failed'):
        async with plugin:
            pass

    assert not plugin.active
    playwright_context_manager.__aexit__.assert_awaited_once()

    stagehand_client.__aexit__.side_effect = None
    async with plugin:
        assert plugin.active


async def test_enter_during_exit_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plugin can't be entered again until its exit has stopped Playwright."""
    plugin = StagehandBrowserPlugin()
    stop_started = asyncio.Event()
    release_stop = asyncio.Event()

    async def wait_for_release(*_: object) -> None:
        stop_started.set()
        await release_stop.wait()

    playwright_context_manager = AsyncMock()
    playwright_context_manager.__aexit__.side_effect = wait_for_release
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=AsyncMock()))

    await plugin.__aenter__()
    exit_task = asyncio.create_task(plugin.__aexit__(None, None, None))
    await stop_started.wait()

    with pytest.raises(RuntimeError, match='already active'):
        await plugin.__aenter__()

    release_stop.set()
    await exit_task


@pytest.mark.parametrize(
    'error',
    [
        pytest.param(RuntimeError('playwright failed'), id='failed'),
        pytest.param(asyncio.CancelledError(), id='cancelled'),
    ],
)
async def test_failed_playwright_start_frees_plugin(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """A plugin whose Playwright fails to start or is cancelled stops it, stays inactive and can be entered again."""
    plugin = StagehandBrowserPlugin()
    failing_context_manager = AsyncMock()
    failing_context_manager.__aenter__.side_effect = error
    monkeypatch.setattr(plugin, '_playwright_context_manager', failing_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=AsyncMock()))

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
    caplog.set_level(logging.DEBUG, logger='crawlee.browsers._stagehand_browser_plugin')
    plugin = StagehandBrowserPlugin()
    playwright_context_manager = AsyncMock()
    playwright_context_manager.__aexit__.side_effect = RuntimeError('stop failed')
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    stagehand_client = AsyncMock()
    stagehand_client.__aenter__.side_effect = RuntimeError('stagehand failed')
    monkeypatch.setattr(
        'crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=stagehand_client)
    )

    with pytest.raises(RuntimeError, match='stagehand failed'):
        async with plugin:
            pass

    assert not plugin.active
    assert 'Stopping Playwright failed while rolling back the plugin entry.' in caplog.text


async def test_hanging_rollback_stop_keeps_enter_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry rollback stops waiting for a Playwright that doesn't stop, so the entry still raises its own error."""
    monkeypatch.setattr(StagehandBrowserPlugin, '_PLAYWRIGHT_STOP_TIMEOUT', timedelta(milliseconds=10))
    plugin = StagehandBrowserPlugin()

    async def hang(*_: object) -> None:
        await asyncio.Event().wait()

    playwright_context_manager = AsyncMock()
    playwright_context_manager.__aexit__.side_effect = hang
    monkeypatch.setattr(plugin, '_playwright_context_manager', playwright_context_manager)
    monkeypatch.setattr('crawlee.browsers._stagehand_browser_plugin.async_playwright', Mock(return_value=AsyncMock()))
    stagehand_client = AsyncMock()
    stagehand_client.__aenter__.side_effect = RuntimeError('stagehand failed')
    monkeypatch.setattr(
        'crawlee.browsers._stagehand_browser_plugin.AsyncStagehand', Mock(return_value=stagehand_client)
    )

    with pytest.raises(RuntimeError, match='stagehand failed'):
        await asyncio.wait_for(plugin.__aenter__(), timeout=10)

    assert not plugin.active
