from __future__ import annotations

import logging
import sys
from unittest.mock import patch

import pytest

from crawlee.autoscaling import ConcurrencySystem
from crawlee.browsers import BrowserPool
from crawlee.crawlers import AdaptivePlaywrightCrawler
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import DefaultRenderingTypePredictor


async def test_accepts_concurrency_system() -> None:
    """A supplied concurrency system is used as is, since the lower default concurrency applies only without one."""
    concurrency_system = ConcurrencySystem()
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(concurrency_system=concurrency_system)

    assert crawler.concurrency_system is concurrency_system


async def test_run_needs_entered_predictor() -> None:
    """A rendering type predictor passed to the crawler has to be entered before the crawler runs."""
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=DefaultRenderingTypePredictor()
    )

    with pytest.raises(RuntimeError, match='DefaultRenderingTypePredictor passed to the crawler is not active'):
        await crawler.run(['https://a.placeholder.com'])


async def test_run_needs_entered_browser_pool() -> None:
    """A browser pool passed to the Playwright sub crawler has to be entered before the crawler runs."""
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        playwright_crawler_specific_kwargs={'browser_pool': BrowserPool()}
    )

    with pytest.raises(RuntimeError, match='BrowserPool passed to the crawler is not active'):
        await crawler.run(['https://a.placeholder.com'])


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


def test_sub_crawlers_log_errors_under_crawler_logger() -> None:
    """The static and Playwright sub crawlers log errors only, under children of the adaptive crawler's logger."""
    logger = logging.getLogger('tests.adaptive_sub_crawler_loggers')
    AdaptivePlaywrightCrawler.with_parsel_static_parser(logger=logger)

    assert logging.getLogger(f'{logger.name}.static').level == logging.ERROR
    assert logging.getLogger(f'{logger.name}.playwright').level == logging.ERROR


def test_default_statistics_log_to_given_logger() -> None:
    """The default statistics of a crawler given a `logger` log their periodic message to it."""
    logger = logging.getLogger('tests.adaptive_statistics_logger')
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(logger=logger)

    assert crawler.statistics._periodic_message_logger is logger


def test_default_statistics_log_to_default_crawler_logger() -> None:
    """The default statistics of a crawler without a `logger` log their periodic message to the crawler's logger."""
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser()

    assert crawler.statistics._periodic_message_logger is crawler.log
