from __future__ import annotations

import sys
from unittest import mock

import pytest

from crawlee import (
    service_locator,
)
from crawlee.autoscaling import ConcurrencySystem
from crawlee.browsers import BrowserPool
from crawlee.configuration import Configuration
from crawlee.crawlers import (
    BasicCrawler,
    PlaywrightCrawler,
)


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


async def test_run_needs_entered_browser_pool() -> None:
    """A browser pool passed to the crawler has to be entered before the crawler runs."""
    crawler = PlaywrightCrawler(browser_pool=BrowserPool())

    with pytest.raises(RuntimeError, match='BrowserPool passed to the crawler is not active'):
        await crawler.run(['https://a.placeholder.com'])


def test_unsupported_browser_type_takes_no_id() -> None:
    """A crawler with a browser type its default fingerprint generator doesn't support doesn't use up a crawler id."""
    first_crawler = BasicCrawler()

    with pytest.raises(ValueError, match='Unsupported browser type'):
        PlaywrightCrawler(browser_type='edge')  # ty: ignore[invalid-argument-type]

    second_crawler = BasicCrawler()
    assert int(second_crawler._id) == int(first_crawler._id) + 1


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
