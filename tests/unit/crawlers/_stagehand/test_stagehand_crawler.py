from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from crawlee.browsers import (
    BrowserPool,
    PlaywrightBrowserPlugin,
    StagehandBrowserPlugin,
    StagehandOptions,
)
from crawlee.crawlers import (
    StagehandCrawler,
)


def test_init_raises_with_browser_pool_and_params() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin()])

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, stagehand_options=StagehandOptions())

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, headless=True)

    with pytest.raises(ValueError, match=r'Cannot specify'):
        StagehandCrawler(browser_pool=pool, browser_launch_options={'headless': True})


def test_init_raises_with_mixed_plugins_in_browser_pool() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin(), PlaywrightBrowserPlugin()])

    with pytest.raises(ValueError, match=r'All BrowserPool plugins must be StagehandBrowserPlugin instances'):
        StagehandCrawler(browser_pool=pool)


def test_init_with_valid_browser_pool() -> None:
    pool = BrowserPool(plugins=[StagehandBrowserPlugin()])
    StagehandCrawler(browser_pool=pool)


def test_forwards_save_response_cookies() -> None:
    """`save_response_cookies` reaches the `PlaywrightCrawler` that saves the cookies of the page."""
    crawler = StagehandCrawler(save_response_cookies=False)

    assert crawler._save_response_cookies is False


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None for mod_name in sys.modules if mod_name == 'stagehand' or mod_name.startswith('stagehand.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith(('crawlee.crawlers._stagehand', 'crawlee.browsers._stagehand_browser_plugin')):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._stagehand import StagehandCrawler  # noqa: F401 PLC0415
