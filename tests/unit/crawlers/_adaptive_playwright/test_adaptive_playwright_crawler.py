from __future__ import annotations

import logging
from datetime import timedelta
from itertools import cycle
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock, patch

import pytest
from typing_extensions import override

from crawlee import Request
from crawlee._types import BasicCrawlingContext
from crawlee.crawlers import BasicCrawler, PlaywrightPreNavCrawlingContext
from crawlee.crawlers._adaptive_playwright import AdaptivePlaywrightCrawler, AdaptivePlaywrightCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatistics,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import AdaptiveContextError
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    RenderingType,
    RenderingTypePrediction,
    RenderingTypePredictor,
)
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from collections.abc import Iterator


class _SimpleRenderingTypePredictor(RenderingTypePredictor):
    """Simplified predictor for tests."""

    def __init__(
        self,
        rendering_types: Iterator[RenderingType] | None = None,
        detection_probability_recommendation: None | Iterator[int] = None,
    ) -> None:
        self._rendering_types = rendering_types or cycle(['static'])
        self._detection_probability_recommendation = detection_probability_recommendation or cycle([1])

    @override
    def predict(self, url: str, label: str | None) -> RenderingTypePrediction:
        return RenderingTypePrediction(next(self._rendering_types), next(self._detection_probability_recommendation))

    @override
    def store_result(self, url: str, label: str | None, crawl_type: RenderingType) -> None:
        pass


@pytest.mark.parametrize(
    ('expected_pw_count', 'expected_bs_count', 'rendering_types', 'detection_probability_recommendation'),
    [
        pytest.param(0, 2, cycle(['static']), cycle([0]), id='Static only'),
        pytest.param(2, 0, cycle(['client only']), cycle([0]), id='Client only'),
        pytest.param(1, 1, cycle(['static', 'client only']), cycle([0]), id='Mixed'),
        pytest.param(2, 2, cycle(['static', 'client only']), cycle([1]), id='Enforced rendering type detection'),
    ],
)
async def test_adaptive_crawling(
    expected_pw_count: int,
    expected_bs_count: int,
    rendering_types: Iterator[RenderingType],
    detection_probability_recommendation: Iterator[int],
) -> None:
    """Tests correct routing to pre-nav hooks and correct handling through proper handler."""
    requests = [
        'https://warehouse-theme-metal.myshopify.com/',
        'https://warehouse-theme-metal.myshopify.com/collections',
    ]

    predictor = _SimpleRenderingTypePredictor(
        rendering_types=rendering_types, detection_probability_recommendation=detection_probability_recommendation
    )

    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=predictor)

    pw_handler_count = 0
    bs_handler_count = 0

    pw_hook_count = 0
    bs_hook_count = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal pw_handler_count
        nonlocal bs_handler_count

        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            pw_handler_count += 1
        except AdaptiveContextError:
            bs_handler_count += 1

    @crawler.pre_navigation_hook_bs
    async def bs_hook(context: BasicCrawlingContext) -> None:  # noqa:ARG001  # Intentionally unused arg
        nonlocal bs_hook_count
        bs_hook_count += 1

    @crawler.pre_navigation_hook_pw
    async def pw_hook(context: PlaywrightPreNavCrawlingContext) -> None:  # noqa:ARG001  # Intentionally unused arg
        nonlocal pw_hook_count
        pw_hook_count += 1

    await crawler.run(requests)

    assert pw_handler_count == expected_pw_count
    assert pw_hook_count == expected_pw_count

    assert bs_handler_count == expected_bs_count
    assert bs_hook_count == expected_bs_count


async def test_adaptive_crawling_context() -> None:
    """Tests that correct context is used. Enforced rendering type detection to run both sub crawlers."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    requests = ['https://warehouse-theme-metal.myshopify.com/']
    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_predictor_enforce_detection)

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        assert context.request.url == requests[0]

    @crawler.pre_navigation_hook_bs
    async def bs_hook(context: BasicCrawlingContext) -> None:
        assert type(context) is BasicCrawlingContext
        assert context.request.url == requests[0]

    @crawler.pre_navigation_hook_pw
    async def pw_hook(context: PlaywrightPreNavCrawlingContext) -> None:
        assert type(context) is PlaywrightPreNavCrawlingContext
        assert context.request.url == requests[0]

    await crawler.run(requests)


async def test_adaptive_crawling_result() -> None:
    """Tests that result only from one sub crawler is saved.

    Enforced rendering type detection to run both sub crawlers."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    requests = ['https://warehouse-theme-metal.myshopify.com/']
    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_predictor_enforce_detection)

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            await context.push_data({'handler': 'pw'})
        except AdaptiveContextError:
            await context.push_data({'handler': 'bs'})

    await crawler.run(requests)

    dataset = await crawler.get_dataset()
    items = [item async for item in dataset.iterate_items()]

    # Enforced rendering type detection will trigger both sub crawlers, but only pw crawler result is saved.
    assert items == [{'handler': 'pw'}]


@pytest.mark.parametrize(
    ('pw_saved_data', 'bs_saved_data', 'expected_result_renderingl_type'),
    [
        pytest.param({'some': 'data'}, {'some': 'data'}, 'static', id='Same results from sub crawlers'),
        pytest.param({'some': 'data'}, {'different': 'data'}, 'client only', id='Different results from sub crawlers'),
    ],
)
async def test_adaptive_crawling_predictor_calls(
    pw_saved_data: dict[str, str], bs_saved_data: dict[str, str], expected_result_renderingl_type: RenderingType
) -> None:
    """Tests expected predictor calls. Same results."""
    some_label = 'bla'
    some_url = 'https://warehouse-theme-metal.myshopify.com/'
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    requests = [Request.from_url(url=some_url, label=some_label)]
    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_predictor_enforce_detection)

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            await context.push_data(pw_saved_data)
        except AdaptiveContextError:
            await context.push_data(bs_saved_data)

    with (
        patch.object(static_only_predictor_enforce_detection, 'store_result', Mock()) as mocked_store_result,
        patch.object(
            static_only_predictor_enforce_detection, 'predict', Mock(return_value=RenderingTypePrediction('static', 1))
        ) as mocked_predict,
    ):
        await crawler.run(requests)

    mocked_predict.assert_called_once_with(some_url, some_label)
    # If `static` and `client only` results are same, `store_result` should be called with `static`.
    mocked_store_result.assert_called_once_with(some_url, some_label, expected_result_renderingl_type)


async def test_adaptive_crawling_result_use_state_isolation() -> None:
    """Tests that global state accessed through `use_state` is changed only by one sub crawler.

    Enforced rendering type detection to run both sub crawlers."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    requests = ['https://warehouse-theme-metal.myshopify.com/']
    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_predictor_enforce_detection)
    store = await crawler.get_key_value_store()
    await store.set_value(BasicCrawler.CRAWLEE_STATE_KEY, {'counter': 0})
    request_handler_calls = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal request_handler_calls
        state = cast(dict[str, int], await context.use_state())
        request_handler_calls += 1
        state['counter'] += 1

    await crawler.run(requests)

    await store.persist_autosaved_values()

    # Request handler was called twice
    assert request_handler_calls == 2
    # Increment of global state happened only once
    assert (await store.get_value(BasicCrawler.CRAWLEE_STATE_KEY))['counter'] == 1


async def test_adaptive_crawling_statistics() -> None:
    """Test adaptive crawling related statistics.

    Crawler set to static crawling, but due to result_checker returning False on static crawling result it
    will do browser crawling instead well. This increments all three adaptive crawling related stats."""
    requests = ['https://warehouse-theme-metal.myshopify.com/']

    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))

    crawler = AdaptivePlaywrightCrawler(
        rendering_type_predictor=static_only_predictor_no_detection,
        result_checker=lambda result: False,  #  noqa: ARG005  # Intentionally unused argument.
    )

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        pass

    await crawler.run(requests)

    assert crawler.predictor_state.http_only_request_handler_runs == 1
    assert crawler.predictor_state.browser_request_handler_runs == 1
    assert crawler.predictor_state.rendering_type_mispredictions == 1


def test_adaptive_default_hooks_raise_exception() -> None:
    """Trying to attach usual pre-navigation hook raises exception.

    It is ambiguous and so sub crawler specific hooks should be used instead."""

    crawler = AdaptivePlaywrightCrawler()

    with pytest.raises(RuntimeError):

        @crawler.pre_navigation_hook
        async def some_hook(whatever: Any) -> None:
            pass


@pytest.mark.parametrize(
    'error_in_pw_crawler',
    [
        pytest.param(False, id='Error only in bs sub crawler'),
        pytest.param(True, id='Error in both sub crawlers'),
    ],
)
async def test_adaptive_crawler_exceptions_in_sub_crawlers(*, error_in_pw_crawler: bool) -> None:
    """Test that correct results are commited when exceptions are raised in sub crawlers.

    Exception in bs sub crawler will be logged and pw sub crawler used instead.
    Any result from bs sub crawler will be discarded, result form pw crawler will be saved instead.
    (But global state modifications through `use_state` will not be reverted!!!)

    Exception in pw sub crawler will prevent any result from being commited. Even if `push_data` was called before
    the exception
    """
    requests = ['https://warehouse-theme-metal.myshopify.com/']
    static_only_no_detection_predictor = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))

    crawler = AdaptivePlaywrightCrawler(rendering_type_predictor=static_only_no_detection_predictor)
    saved_data = {'some': 'data'}

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            await context.push_data(saved_data)
            if error_in_pw_crawler:
                raise RuntimeError('Some pw sub crawler related error')

        except AdaptiveContextError:
            await context.push_data({'this': 'data should not be saved'})
            raise RuntimeError('Some bs sub crawler related error') from None

    await crawler.run(requests)

    dataset = await crawler.get_dataset()
    stored_results = [item async for item in dataset.iterate_items()]

    if error_in_pw_crawler:
        assert stored_results == []
    else:
        assert stored_results == [saved_data]


def test_adaptive_playwright_crawler_statistics_in_init() -> None:
    """Tests that adaptive crawler uses created AdaptivePlaywrightCrawlerStatistics from inputted Statistics."""
    persistence_enabled = True
    persist_state_kvs_name = 'some name'
    persist_state_key = 'come key'
    log_message = 'some message'
    periodic_message_logger = logging.getLogger('some logger')  # Accessing private member to create copy like-object.
    log_interval = timedelta(minutes=2)
    statistics = Statistics(
        persistence_enabled=persistence_enabled,
        persist_state_kvs_name=persist_state_kvs_name,
        persist_state_key=persist_state_key,
        log_message=log_message,
        periodic_message_logger=periodic_message_logger,
        log_interval=log_interval,
    )

    crawler = AdaptivePlaywrightCrawler(statistics=statistics)

    assert type(crawler._statistics) is AdaptivePlaywrightCrawlerStatistics
    assert crawler._statistics._persistence_enabled == persistence_enabled
    assert crawler._statistics._persist_state_kvs_name == persist_state_kvs_name
    assert crawler._statistics._persist_state_key == persist_state_key
    assert crawler._statistics._log_message == log_message
    assert crawler._statistics._periodic_message_logger == periodic_message_logger
    assert crawler._statistics._log_interval == log_interval
