from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from itertools import cycle
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock, call, patch

import httpx
import pytest
from bs4 import Tag
from parsel import Selector
from typing_extensions import override

from crawlee import Request
from crawlee.browsers import BrowserPool
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
    BasicCrawler,
    RenderingType,
    RenderingTypePrediction,
    RenderingTypePredictor,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatisticState,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptiveContextError,
)
from crawlee.statistics import Statistics
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    import respx

    from crawlee.browsers._browser_plugin import BrowserPlugin
    from crawlee.browsers._types import CrawleePage
    from crawlee.proxy_configuration import ProxyInfo


_H1_TEXT = 'Static'
_H2_TEXT = 'Only in browser'
_H3_CHANGED_TEXT = 'Changed by JS'
_INJECTED_JS_DELAY_MS = 100
_PAGE_CONTENT_STATIC = f"""
<h1>{_H1_TEXT}</h1>
<h3>Initial text</h3>
<script>
    setTimeout(function() {{
    let h2 = document.createElement('h2');
    h2.innerText = "{_H2_TEXT}";
    document.getElementsByTagName("body")[0].append(h2);
    document.getElementsByTagName("h3")[0].textContent="{_H3_CHANGED_TEXT}";
    }}, {_INJECTED_JS_DELAY_MS});

</script>
"""


@pytest.fixture
def test_urls(respx_mock: respx.MockRouter) -> list[str]:
    """Example pages used in the test are mocked for static requests."""
    urls = [
        'https://warehouse-theme-metal.myshopify.com/',
        'https://warehouse-theme-metal.myshopify.com/collections',
    ]

    for url in urls:
        respx_mock.get(url).return_value = httpx.Response(status_code=200, content=_PAGE_CONTENT_STATIC.encode())
    return urls


@pytest.fixture
async def key_value_store() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open()
    yield kvs
    await kvs.drop()


class _StaticRedirectBrowserPool(BrowserPool):
    """BrowserPool for redirecting browser requests to static content."""

    async def new_page(
        self,
        *,
        page_id: str | None = None,
        browser_plugin: BrowserPlugin | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> CrawleePage:
        crawlee_page = await super().new_page(page_id=page_id, browser_plugin=browser_plugin, proxy_info=proxy_info)
        await crawlee_page.page.route(
            '**/*',
            lambda route: route.fulfill(status=200, content_type='text/html', body=_PAGE_CONTENT_STATIC),
        )
        return crawlee_page


class _SimpleRenderingTypePredictor(RenderingTypePredictor):
    """Simplified predictor for tests."""

    def __init__(
        self,
        rendering_types: Iterator[RenderingType] | None = None,
        detection_probability_recommendation: None | Iterator[float] = None,
    ) -> None:
        self._rendering_types = rendering_types or cycle(['static'])
        self._detection_probability_recommendation = detection_probability_recommendation or cycle([1])

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:
        return RenderingTypePrediction(next(self._rendering_types), next(self._detection_probability_recommendation))

    @override
    def store_result(self, request: Request, crawl_type: RenderingType) -> None:
        pass


@dataclass(frozen=True)
class _TestInput:
    expected_pw_count: int
    expected_static_count: int
    rendering_types: Iterator[RenderingType]
    detection_probability_recommendation: Iterator[float]


@pytest.mark.parametrize(
    'test_input',
    [
        pytest.param(
            _TestInput(
                expected_pw_count=0,
                expected_static_count=2,
                rendering_types=cycle(['static']),
                detection_probability_recommendation=cycle([0]),
            ),
            id='Static only',
        ),
        pytest.param(
            _TestInput(
                expected_pw_count=2,
                expected_static_count=0,
                rendering_types=cycle(['client only']),
                detection_probability_recommendation=cycle([0]),
            ),
            id='Client only',
        ),
        pytest.param(
            _TestInput(
                expected_pw_count=1,
                expected_static_count=1,
                rendering_types=cycle(['static', 'client only']),
                detection_probability_recommendation=cycle([0]),
            ),
            id='Mixed',
        ),
        pytest.param(
            _TestInput(
                expected_pw_count=2,
                expected_static_count=2,
                rendering_types=cycle(['static', 'client only']),
                detection_probability_recommendation=cycle([1]),
            ),
            id='Enforced rendering type detection',
        ),
    ],
)
async def test_adaptive_crawling(
    test_input: _TestInput,
    test_urls: list[str],
) -> None:
    """Tests correct routing to pre-nav hooks and correct handling through proper handler."""

    predictor = _SimpleRenderingTypePredictor(
        rendering_types=test_input.rendering_types,
        detection_probability_recommendation=test_input.detection_probability_recommendation,
    )

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=predictor,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    pw_handler_count = 0
    static_handler_count = 0

    pw_hook_count = 0
    static_hook_count = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal pw_handler_count
        nonlocal static_handler_count

        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            pw_handler_count += 1
        except AdaptiveContextError:
            static_handler_count += 1

    @crawler.pre_navigation_hook
    async def pre_nav_hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:  # Intentionally unused arg
        nonlocal static_hook_count
        nonlocal pw_hook_count

        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            pw_hook_count += 1
        except AdaptiveContextError:
            static_hook_count += 1

    await crawler.run(test_urls)

    assert pw_handler_count == test_input.expected_pw_count
    assert pw_hook_count == test_input.expected_pw_count

    assert static_handler_count == test_input.expected_static_count
    assert static_hook_count == test_input.expected_static_count


async def test_adaptive_crawling_parsel(test_urls: list[str]) -> None:
    """Top level test for parsel. Only one argument combination. (The rest of code is tested with bs variant.)"""
    predictor = _SimpleRenderingTypePredictor(
        rendering_types=cycle(['static', 'client only']), detection_probability_recommendation=cycle([0])
    )

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        rendering_type_predictor=predictor,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    pw_handler_count = 0
    static_handler_count = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal pw_handler_count
        nonlocal static_handler_count

        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            pw_handler_count += 1
        except AdaptiveContextError:
            static_handler_count += 1

    await crawler.run(test_urls)

    assert pw_handler_count == 1
    assert static_handler_count == 1


async def test_adaptive_crawling_pre_nav_change_to_context(test_urls: list[str]) -> None:
    """Tests that context can be modified in pre-navigation hooks."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_enforce_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
    user_data_in_pre_nav_hook = []
    user_data_in_handler = []

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        user_data_in_handler.append(context.request.user_data.get('data', None))

    @crawler.pre_navigation_hook
    async def pre_nav_hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        user_data_in_pre_nav_hook.append(context.request.user_data.get('data', None))
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            context.request.user_data['data'] = 'pw'
        except AdaptiveContextError:
            context.request.user_data['data'] = 'bs'

    await crawler.run(test_urls[:1])
    # Check that repeated pre nav hook invocations do not influence each other while probing
    assert user_data_in_pre_nav_hook == [None, None]
    # Check that the request handler sees changes to user data done by pre nav hooks
    assert user_data_in_handler == ['pw', 'bs']


async def test_playwright_only_hook(test_urls: list[str]) -> None:
    """Test that hook can be registered for playwright only sub crawler.

    Create a situation where one page is crawled by both sub crawlers. One common pre navigation hook is registered and
    one playwright only pre navigation hook is registered."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_enforce_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
    pre_nav_hook_common = Mock()
    pre_nav_hook_playwright = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        pass

    @crawler.pre_navigation_hook
    async def pre_nav_hook(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        pre_nav_hook_common(context.request.url)

    @crawler.pre_navigation_hook(playwright_only=True)
    async def pre_nav_hook_pw_only(context: AdaptivePlaywrightPreNavCrawlingContext) -> None:
        pre_nav_hook_playwright(context.page.url)

    await crawler.run(test_urls[:1])

    # Default behavior. Hook is called everytime, both static sub crawler and playwright sub crawler.
    pre_nav_hook_common.assert_has_calls([call(test_urls[0]), call(test_urls[0])])
    # Hook is called only by playwright sub crawler.
    pre_nav_hook_playwright.assert_called_once_with('about:blank')


async def test_adaptive_crawling_result(test_urls: list[str]) -> None:
    """Tests that result only from one sub crawler is saved.

    Enforced rendering type detection to run both sub crawlers."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_enforce_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            await context.push_data({'handler': 'pw'})
        except AdaptiveContextError:
            await context.push_data({'handler': 'bs'})

    await crawler.run(test_urls[:1])

    # Enforced rendering type detection will trigger both sub crawlers, but only pw crawler result is saved.
    assert (await crawler.get_data()).items == [{'handler': 'pw'}]


@pytest.mark.parametrize(
    ('pw_saved_data', 'static_saved_data', 'expected_result_rendering_type'),
    [
        pytest.param({'some': 'data'}, {'some': 'data'}, 'static', id='Same results from sub crawlers'),
        pytest.param({'some': 'data'}, {'different': 'data'}, 'client only', id='Different results from sub crawlers'),
    ],
)
async def test_adaptive_crawling_predictor_calls(
    pw_saved_data: dict[str, str],
    static_saved_data: dict[str, str],
    expected_result_rendering_type: RenderingType,
    test_urls: list[str],
) -> None:
    """Tests expected predictor calls. Same results."""
    some_label = 'bla'
    some_url = test_urls[0]
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    requests = [Request.from_url(url=some_url, label=some_label)]
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_enforce_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            await context.push_data(pw_saved_data)
        except AdaptiveContextError:
            await context.push_data(static_saved_data)

    with (
        patch.object(static_only_predictor_enforce_detection, 'store_result', Mock()) as mocked_store_result,
        patch.object(
            static_only_predictor_enforce_detection, 'predict', Mock(return_value=RenderingTypePrediction('static', 1))
        ) as mocked_predict,
    ):
        await crawler.run(requests)

    mocked_predict.assert_called_once_with(requests[0])
    # If `static` and `client only` results are same, `store_result` should be called with `static`.
    mocked_store_result.assert_called_once_with(requests[0], expected_result_rendering_type)


async def test_adaptive_crawling_result_use_state_isolation(
    key_value_store: KeyValueStore, test_urls: list[str]
) -> None:
    """Tests that global state accessed through `use_state` is changed only by one sub crawler.

    Enforced rendering type detection to run both sub crawlers."""
    static_only_predictor_enforce_detection = _SimpleRenderingTypePredictor()
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_enforce_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
    await key_value_store.set_value(BasicCrawler._CRAWLEE_STATE_KEY, {'counter': 0})
    request_handler_calls = 0

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        nonlocal request_handler_calls
        state = cast('dict[str, int]', await context.use_state())
        request_handler_calls += 1
        state['counter'] += 1

    await crawler.run(test_urls[:1])

    await key_value_store.persist_autosaved_values()

    # Request handler was called twice
    assert request_handler_calls == 2
    # Increment of global state happened only once
    assert (await key_value_store.get_value(BasicCrawler._CRAWLEE_STATE_KEY))['counter'] == 1


async def test_adaptive_crawling_statistics(test_urls: list[str]) -> None:
    """Test adaptive crawler statistics.

    Crawler set to static crawling, but due to result_checker returning False on static crawling result it
    will do browser crawling instead as well. This increments all three adaptive crawling related stats."""
    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_predictor_no_detection,
        result_checker=lambda result: False,  #  noqa: ARG005  # Intentionally unused argument.
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        pass

    await crawler.run(test_urls[:1])

    assert crawler.statistics.state.http_only_request_handler_runs == 1
    assert crawler.statistics.state.browser_request_handler_runs == 1
    assert crawler.statistics.state.rendering_type_mispredictions == 1

    # Despite running both sub crawlers the top crawler statistics should count this as one request finished.
    assert crawler.statistics.state.requests_finished == 1
    assert crawler.statistics.state.requests_failed == 0


@pytest.mark.parametrize(
    'error_in_pw_crawler',
    [
        pytest.param(False, id='Error only in static sub crawler'),
        pytest.param(True, id='Error in both sub crawlers'),
    ],
)
async def test_adaptive_crawler_exceptions_in_sub_crawlers(*, error_in_pw_crawler: bool, test_urls: list[str]) -> None:
    """Test that correct results are commited when exceptions are raised in sub crawlers.

    Exception in bs sub crawler will be logged and pw sub crawler used instead.
    Any result from bs sub crawler will be discarded, result form pw crawler will be saved instead.
    (But global state modifications through `use_state` will not be reverted!!!)

    Exception in pw sub crawler will prevent any result from being commited. Even if `push_data` was called before
    the exception
    """
    static_only_no_detection_predictor = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        rendering_type_predictor=static_only_no_detection_predictor,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
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

    await crawler.run(test_urls[:1])

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
    statistics = Statistics.with_default_state(
        persistence_enabled=persistence_enabled,
        persist_state_kvs_name=persist_state_kvs_name,
        persist_state_key=persist_state_key,
        log_message=log_message,
        periodic_message_logger=periodic_message_logger,
        log_interval=log_interval,
    )

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(statistics=statistics)

    assert type(crawler._statistics.state) is AdaptivePlaywrightCrawlerStatisticState
    assert crawler._statistics._persistence_enabled == persistence_enabled
    assert crawler._statistics._persist_state_kvs_name == persist_state_kvs_name
    assert crawler._statistics._persist_state_key == persist_state_key
    assert crawler._statistics._log_message == log_message
    assert crawler._statistics._periodic_message_logger == periodic_message_logger


async def test_adaptive_playwright_crawler_timeout_in_sub_crawler(test_urls: list[str]) -> None:
    """Tests that timeout in static sub crawler forces fall back to browser sub crawler.

    Create situation where static sub crawler blocks(should time out), such error should start browser sub
    crawler."""

    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))
    request_handler_timeout = timedelta(seconds=0.1)

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_request_retries=1,
        rendering_type_predictor=static_only_predictor_no_detection,
        request_handler_timeout=request_handler_timeout,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
    mocked_static_handler = Mock()
    mocked_browser_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            mocked_browser_handler()
        except AdaptiveContextError:
            mocked_static_handler()
            # Relax timeout for the fallback browser request to avoid flakiness in test
            crawler._request_handler_timeout = timedelta(seconds=5)
            # Sleep for time obviously larger than top crawler timeout.
            await asyncio.sleep(request_handler_timeout.total_seconds() * 2)

    await crawler.run(test_urls[:1])

    mocked_static_handler.assert_called_once_with()
    # Browser handler was capable of running despite static handler having sleep time larger than top handler timeout.
    mocked_browser_handler.assert_called_once_with()


async def test_adaptive_playwright_crawler_default_predictor(test_urls: list[str]) -> None:
    """Test default rendering type predictor integration into crawler."""

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )
    mocked_static_handler = Mock()
    mocked_browser_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        try:
            # page is available only if it was crawled by PlaywrightCrawler.
            context.page  # noqa:B018 Intentionally "useless expression". Can trigger exception.
            mocked_browser_handler()
        except AdaptiveContextError:
            mocked_static_handler()

    await crawler.run(test_urls[:1])

    # First prediction should trigger rendering type detection as the predictor does not have any data for prediction.
    mocked_static_handler.assert_called_once_with()
    mocked_browser_handler.assert_called_once_with()


async def test_adaptive_context_query_selector_beautiful_soup(test_urls: list[str]) -> None:
    """Test that `context.query_selector_one` works regardless of the crawl type for BeautifulSoup variant.

    Handler tries to locate two elements h1 and h2.
    h1 exists immediately, h2 is created dynamically by inline JS snippet embedded in the html.
    Create situation where page is crawled with static sub crawler first.
    Static sub crawler should be able to locate only h1. It wil try to wait for h2, trying to wait for h2 will trigger
    `AdaptiveContextError` which will force the adaptive crawler to try playwright sub crawler instead. Playwright sub
    crawler is able to wait for the h2 element."""

    # Get page with injected JS code that will add some element after timeout
    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_request_retries=1,
        rendering_type_predictor=static_only_predictor_no_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    mocked_h1_handler = Mock()
    mocked_h2_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        h1 = await context.query_selector_one('h1', timedelta(milliseconds=_INJECTED_JS_DELAY_MS * 2))
        mocked_h1_handler(h1)
        h2 = await context.query_selector_one('h2', timedelta(milliseconds=_INJECTED_JS_DELAY_MS * 2))
        mocked_h2_handler(h2)

    await crawler.run(test_urls[:1])

    expected_h1_tag = Tag(name='h1')
    expected_h1_tag.append(_H1_TEXT)

    expected_h2_tag = Tag(name='h2')
    expected_h2_tag.append(_H2_TEXT)

    # Called by both sub crawlers
    mocked_h1_handler.assert_has_calls([call(expected_h1_tag), call(expected_h1_tag)])
    # Called only by pw sub crawler
    mocked_h2_handler.assert_has_calls([call(expected_h2_tag)])


async def test_adaptive_context_query_selector_parsel(test_urls: list[str]) -> None:
    """Test that `context.query_selector_one` works regardless of the crawl type for Parsel variant.

    Handler tries to locate two elements h1 and h2.
    h1 exists immediately, h2 is created dynamically by inline JS snippet embedded in the html.
    Create situation where page is crawled with static sub crawler first.
    Static sub crawler should be able to locate only h1. It wil try to wait for h2, trying to wait for h2 will trigger
    `AdaptiveContextError` which will force the adaptive crawler to try playwright sub crawler instead. Playwright sub
    crawler is able to wait for the h2 element."""

    # Get page with injected JS code that will add some element after timeout
    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))
    expected_h1_tag = f'<h1>{_H1_TEXT}</h1>'
    expected_h2_tag = f'<h2>{_H2_TEXT}</h2>'

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        max_request_retries=1,
        rendering_type_predictor=static_only_predictor_no_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    mocked_h1_handler = Mock()
    mocked_h2_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        if h1 := await context.query_selector_one('h1', timedelta(milliseconds=_INJECTED_JS_DELAY_MS * 2)):
            mocked_h1_handler(type(h1), h1.get())
        if h2 := await context.query_selector_one('h2', timedelta(milliseconds=_INJECTED_JS_DELAY_MS * 2)):
            mocked_h2_handler(type(h2), h2.get())

    await crawler.run(test_urls[:1])

    # Called by both sub crawlers
    mocked_h1_handler.assert_has_calls([call(Selector, expected_h1_tag), call(Selector, expected_h1_tag)])
    # Called only by pw sub crawler
    mocked_h2_handler.assert_has_calls([call(Selector, expected_h2_tag)])


async def test_adaptive_context_parse_with_static_parser_parsel(test_urls: list[str]) -> None:
    """Test `context.parse_with_static_parser` works regardless of the crawl type for Parsel variant.

    (Test covers also  `context.wait_for_selector`, which is called by `context.parse_with_static_parser`)
    """
    static_only_predictor_no_detection = _SimpleRenderingTypePredictor(detection_probability_recommendation=cycle([0]))
    expected_h2_tag = f'<h2>{_H2_TEXT}</h2>'

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        max_request_retries=1,
        rendering_type_predictor=static_only_predictor_no_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    mocked_h2_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        h2_static = context.parsed_content.css('h2')  # Should not find anything
        mocked_h2_handler(h2_static)

        # Reparse whole page after h2 appears
        parsed_content_after_h2_appeared = await context.parse_with_static_parser(
            selector='h2', timeout=timedelta(milliseconds=_INJECTED_JS_DELAY_MS * 2)
        )
        mocked_h2_handler(parsed_content_after_h2_appeared.css('h2')[0].get())

    await crawler.run(test_urls[:1])

    mocked_h2_handler.assert_has_calls(
        [
            call([]),  # Static sub crawler tried and did not find h2.
            call([]),  # Playwright sub crawler tried and did not find h2 without waiting.
            call(expected_h2_tag),  # Playwright waited for h2 to appear.
        ]
    )


async def test_adaptive_context_helpers_on_changed_selector(test_urls: list[str]) -> None:
    """Test that context helpers work on latest version of the page.

    Scenario where page is changed after a while. H2 element is added and text of H3 element is modified.
    Test that context helpers automatically work on latest version of the page by reading H3 element and expecting it's
    dynamically changed text instead of the original static text.
    """
    browser_only_predictor_no_detection = _SimpleRenderingTypePredictor(
        rendering_types=cycle(['client only']), detection_probability_recommendation=cycle([0])
    )
    expected_h3_tag = f'<h3>{_H3_CHANGED_TEXT}</h3>'

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        max_request_retries=1,
        rendering_type_predictor=browser_only_predictor_no_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    mocked_h3_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        await context.query_selector_one('h2')  # Wait for change that is indicated by appearance of h2 element.
        if h3 := await context.query_selector_one('h3'):
            mocked_h3_handler(h3.get())  # Get updated h3 element.

    await crawler.run(test_urls[:1])

    mocked_h3_handler.assert_called_once_with(expected_h3_tag)


async def test_adaptive_context_query_non_existing_element(test_urls: list[str]) -> None:
    """Test that querying non-existing selector returns `None`"""
    browser_only_predictor_no_detection = _SimpleRenderingTypePredictor(
        rendering_types=cycle(['client only']), detection_probability_recommendation=cycle([0])
    )

    crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
        max_request_retries=1,
        rendering_type_predictor=browser_only_predictor_no_detection,
        playwright_crawler_specific_kwargs={'browser_pool': _StaticRedirectBrowserPool.with_default_plugin()},
    )

    mocked_h3_handler = Mock()

    @crawler.router.default_handler
    async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
        mocked_h3_handler(await context.query_selector_one('non sense selector', timeout=timedelta(milliseconds=1)))

    await crawler.run(test_urls[:1])

    mocked_h3_handler.assert_called_once_with(None)
