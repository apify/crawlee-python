from __future__ import annotations

import sys
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from crawlee import (
    service_locator,
)
from crawlee.autoscaling import ConcurrencySystem
from crawlee.configuration import Configuration
from crawlee.crawlers import (
    PlaywrightCrawler,
)

if TYPE_CHECKING:
    from crawlee.crawlers import PlaywrightCrawlingContext


async def test_passing_configuration() -> None:
    """Check that the configuration is allowed to be passed to the Playwrightcrawler."""
    service_locator.set_configuration(Configuration(log_level='INFO'))
    configuration = Configuration(log_level='WARNING')

    crawler = PlaywrightCrawler(configuration=configuration)

    assert service_locator.get_configuration().log_level == 'INFO'
    assert crawler._service_locator.get_configuration().log_level == 'WARNING'


async def test_accepts_concurrency_system() -> None:
    """A supplied concurrency system is used as is, since the lower default concurrency applies only without one."""
    concurrency_system = ConcurrencySystem()
    crawler = PlaywrightCrawler(concurrency_system=concurrency_system)

    assert crawler.concurrency_system is concurrency_system


def test_lock_time_counts_navigation() -> None:
    """Test that the expected processing time adds the navigation timeout to the longest request handler timeout."""
    crawler = PlaywrightCrawler(navigation_timeout=timedelta(minutes=1))

    @crawler.router.handler('SLOW', request_handler_timeout=timedelta(minutes=3))
    async def slow_handler(_context: PlaywrightCrawlingContext) -> None:
        pass

    assert crawler._get_expected_processing_time() == timedelta(minutes=5)


def test_import_error_handled() -> None:
    blocked = {
        mod_name: None for mod_name in sys.modules if mod_name == 'playwright' or mod_name.startswith('playwright.')
    }
    with mock.patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith('crawlee.crawlers._playwright'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._playwright import PlaywrightCrawler  # noqa: F401 PLC0415
