from __future__ import annotations

import sys
from unittest.mock import Mock, patch

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


def test_navigation_request_is_not_fetched_by_playwright() -> None:
    """Playwright does not know the proxy of a browser attached over CDP, so it never sends the navigation request."""
    crawler = StagehandCrawler()

    assert crawler._can_fetch_navigation_request(Mock()) is False
