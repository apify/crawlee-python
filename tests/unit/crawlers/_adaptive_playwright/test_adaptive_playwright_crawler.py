from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from crawlee.autoscaling import ConcurrencySystem
from crawlee.crawlers import AdaptivePlaywrightCrawler


async def test_accepts_concurrency_system() -> None:
    """A supplied concurrency system is used as is, since the lower default concurrency applies only without one."""
    concurrency_system = ConcurrencySystem()
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(concurrency_system=concurrency_system)

    assert crawler.concurrency_system is concurrency_system


@pytest.mark.parametrize(
    'optional_module_name',
    [
        pytest.param('playwright', id='playwright'),
        pytest.param('rapidfuzz', id='rapidfuzz'),
        pytest.param('sklearn', id='sklearn'),
    ],
)
def test_import_error_handled(optional_module_name: str) -> None:
    # Block the package and all its cached submodules to prevent submodule cache entries
    # from bypassing the blocked top-level package.
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }

    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name == 'crawlee.crawlers' or mod_name.startswith('crawlee.crawlers._adaptive_playwright'):
                sys.modules.pop(mod_name, None)

        with pytest.raises(ImportError):
            from crawlee.crawlers import AdaptivePlaywrightCrawler  # noqa: F401 PLC0415
