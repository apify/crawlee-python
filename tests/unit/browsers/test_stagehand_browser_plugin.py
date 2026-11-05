from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.browsers import StagehandBrowserPlugin, StagehandOptions

if TYPE_CHECKING:
    import pytest


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
