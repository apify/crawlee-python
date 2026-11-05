from __future__ import annotations

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


async def raise_error_if_chrome_and_executable_path() -> None:
    with pytest.raises(
        ValueError, match=r'Cannot use `use_chrome` with `Configuration.default_browser_path` or `executable_path` set.'
    ):
        PlaywrightBrowserPlugin(
            browser_type='chrome',
            browser_launch_options={'executable_path': '/path/to/chrome'},
        )
