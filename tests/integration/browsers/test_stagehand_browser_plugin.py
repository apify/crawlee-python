from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from stagehand import AsyncStagehand

from crawlee.browsers import StagehandBrowserController, StagehandBrowserPlugin, StagehandOptions

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


_PATCH_MODULE = 'crawlee.browsers._stagehand_browser_plugin'


@pytest.fixture
def stagehand_client_mock() -> MagicMock:
    client = MagicMock(spec=AsyncStagehand)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


@pytest.fixture
async def plugin() -> AsyncGenerator[StagehandBrowserPlugin, None]:
    async with StagehandBrowserPlugin() as plugin:
        yield plugin


async def test_stagehand_called_with_local_params(stagehand_client_mock: MagicMock) -> None:
    with patch(f'{_PATCH_MODULE}.AsyncStagehand', return_value=stagehand_client_mock) as stagehand_mock:
        async with StagehandBrowserPlugin(
            stagehand_options=StagehandOptions(env='LOCAL', local_ready_timeout_s=20.0, model_api_key='test_model_key')
        ):
            pass

    call_kwargs = stagehand_mock.call_args.kwargs
    assert call_kwargs['server'] == 'local'
    assert call_kwargs['local_ready_timeout_s'] == 20.0
    assert call_kwargs['model_api_key'] == 'test_model_key'
    assert 'browserbase_api_key' not in call_kwargs
    assert 'browserbase_project_id' not in call_kwargs

    # In local environment, the plugin should set `local_chrome_path` to the path of the Playwright Chromium executable.
    assert 'local_chrome_path' in call_kwargs


async def test_stagehand_called_with_browserbase_params(stagehand_client_mock: MagicMock) -> None:
    with patch(f'{_PATCH_MODULE}.AsyncStagehand', return_value=stagehand_client_mock) as stagehand_mock:
        async with StagehandBrowserPlugin(
            stagehand_options=StagehandOptions(
                env='BROWSERBASE',
                browserbase_api_key='test_key',
                project_id='test_project_id',
                model_api_key='test_model_key',
            )
        ):
            pass

    call_kwargs = stagehand_mock.call_args.kwargs
    assert call_kwargs['server'] == 'remote'
    assert call_kwargs['browserbase_api_key'] == 'test_key'
    assert call_kwargs['browserbase_project_id'] == 'test_project_id'
    assert call_kwargs['model_api_key'] == 'test_model_key'

    # In Browserbase environment, the plugin should NOT set `local_chrome_path` as it's not used.
    assert 'local_chrome_path' not in call_kwargs


async def test_methods_raise_error_when_not_active() -> None:
    plugin = StagehandBrowserPlugin()

    assert plugin.active is False

    with pytest.raises(RuntimeError, match=r'Plugin is not active'):
        await plugin.new_browser()

    with pytest.raises(RuntimeError, match=r'Plugin is already active.'):
        async with plugin, plugin:
            pass

    async with plugin:
        assert plugin.active is True


async def test_new_browser(plugin: StagehandBrowserPlugin) -> None:
    browser_controller = await plugin.new_browser()

    assert isinstance(browser_controller, StagehandBrowserController)

    assert browser_controller.is_browser_connected
    assert browser_controller.has_free_capacity

    await browser_controller.close()


async def test_multiple_new_browsers(plugin: StagehandBrowserPlugin) -> None:
    browser_controller_1 = await plugin.new_browser()
    browser_controller_2 = await plugin.new_browser()

    assert browser_controller_1 is not browser_controller_2
