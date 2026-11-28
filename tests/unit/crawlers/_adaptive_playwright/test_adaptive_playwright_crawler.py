from __future__ import annotations

import logging
import sys
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import create_autospec, patch

import pytest

from crawlee import service_locator
from crawlee.autoscaling import ConcurrencySystem
from crawlee.browsers import BrowserPool
from crawlee.crawlers import AdaptivePlaywrightCrawler, AdaptivePlaywrightCrawlerStatisticState, BasicCrawler
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import _NonPersistentStatistics
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import DefaultRenderingTypePredictor
from crawlee.events import Event, EventManager, EventPersistStateData
from crawlee.statistics import Statistics
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from crawlee.crawlers import AdaptivePlaywrightCrawlingContext


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
    'factory',
    [
        pytest.param(AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser, id='beautifulsoup'),
        pytest.param(AdaptivePlaywrightCrawler.with_parsel_static_parser, id='parsel'),
    ],
)
def test_factory_passes_stats_as_is(factory: Callable[..., AdaptivePlaywrightCrawler]) -> None:
    """A factory method passes the given statistics to the crawler as they are."""
    statistics = Statistics(state_model=AdaptivePlaywrightCrawlerStatisticState)
    crawler = factory(statistics=statistics)

    assert crawler.statistics is statistics


@pytest.mark.parametrize(
    'factory',
    [
        pytest.param(AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser, id='beautifulsoup'),
        pytest.param(AdaptivePlaywrightCrawler.with_parsel_static_parser, id='parsel'),
    ],
)
def test_rejects_plain_statistics(factory: Callable[..., AdaptivePlaywrightCrawler]) -> None:
    """Statistics without the adaptive state model are rejected when the crawler is created."""
    with pytest.raises(ValueError, match='AdaptivePlaywrightCrawlerStatisticState'):
        factory(statistics=Statistics.with_default_state())


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


async def test_default_statistics_persist_to_crawler_store() -> None:
    """The statistics the crawler creates persist to the crawler's key-value store."""
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(storage_backend=MemoryStorageBackend())

    async with crawler.statistics:
        pass

    key = f'__CRAWLER_STATISTICS_{crawler.statistics.state.stats_id}'
    crawler_kvs = await crawler.get_key_value_store()
    global_kvs = await KeyValueStore.open()
    assert await crawler_kvs.get_value(key) is not None
    assert await global_kvs.get_value(key) is None


async def test_default_predictor_persists_to_crawler_store() -> None:
    """The predictor the crawler creates persists to the crawler's key-value store."""
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(storage_backend=MemoryStorageBackend())

    async with crawler.rendering_type_predictor:
        pass

    crawler_kvs = await crawler.get_key_value_store()
    global_kvs = await KeyValueStore.open()
    assert await crawler_kvs.get_value('rendering-type-predictor-state') is not None
    assert await global_kvs.get_value('rendering-type-predictor-state') is None


async def test_sub_crawler_statistics_do_not_persist() -> None:
    """The statistics of the sub crawlers write nothing on `PERSIST_STATE`."""
    async with EventManager() as event_manager:
        service_locator.set_event_manager(event_manager)

        async with _NonPersistentStatistics():
            event_manager.emit(event=Event.PERSIST_STATE, event_data=EventPersistStateData(is_migrating=False))
            await event_manager.wait_for_all_listeners_to_complete()

    kvs = await KeyValueStore.open()
    assert await kvs.list_keys() == []


def test_lock_time_counts_both_runs() -> None:
    """Test that the expected processing time counts the request handler timeout once for each sub crawler."""
    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser()

    @crawler.router.handler('SLOW', request_handler_timeout=timedelta(minutes=3))
    async def slow_handler(_context: AdaptivePlaywrightCrawlingContext) -> None:
        pass

    assert crawler._get_expected_processing_time() == timedelta(minutes=7)


def test_one_shot_status_codes_reach_sub_crawlers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Status code options given as a one-shot iterable reach the crawler and both of its sub crawlers."""
    init = create_autospec(BasicCrawler.__init__, side_effect=BasicCrawler.__init__)
    monkeypatch.setattr(BasicCrawler, '__init__', init)

    AdaptivePlaywrightCrawler.with_parsel_static_parser(
        blocked_status_codes=(code for code in [402]),
        ignore_http_error_status_codes=(code for code in [418]),
    )

    crawlers = [call.args[0] for call in init.call_args_list]
    assert len(crawlers) == 3
    assert all(crawler._blocked_status_codes == frozenset({402}) for crawler in crawlers)
    assert all(crawler._ignore_http_error_status_codes == {418} for crawler in crawlers)
