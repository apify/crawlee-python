from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from random import random
from typing import TYPE_CHECKING, Any, Generic, get_args

from bs4 import BeautifulSoup, Tag
from parsel import Selector
from typing_extensions import Self, TypeVar, override

from crawlee._types import BasicCrawlingContext, JsonSerializable, RequestHandlerRunResult
from crawlee._utils.docs import docs_group
from crawlee._utils.wait import wait_for
from crawlee.crawlers import (
    AbstractHttpCrawler,
    AbstractHttpParser,
    BasicCrawler,
    BeautifulSoupParserType,
    ParsedHttpCrawlingContext,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
from crawlee.crawlers._parsel._parsel_parser import ParselParser
from crawlee.statistics import Statistics, StatisticsState

from ._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatisticState,
)
from ._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)
from ._rendering_type_predictor import (
    DefaultRenderingTypePredictor,
    RenderingType,
    RenderingTypePredictor,
)
from ._result_comparator import (
    create_default_comparator,
)

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Unpack

    from crawlee.crawlers._basic._basic_crawler import _BasicCrawlerOptions
    from crawlee.crawlers._playwright._playwright_crawler import _PlaywrightCrawlerAdditionalOptions


TStaticParseResult = TypeVar('TStaticParseResult')
TStaticSelectResult = TypeVar('TStaticSelectResult')
TStaticCrawlingContext = TypeVar('TStaticCrawlingContext', bound=ParsedHttpCrawlingContext)


class _NonPersistentStatistics(Statistics):
    """Statistics compliant object that is not supposed to do anything when entering/exiting context.

    To be used in sub crawlers.
    """

    def __init__(self) -> None:
        super().__init__(state_model=StatisticsState)

    async def __aenter__(self) -> Self:
        self._active = True
        await self._state.initialize()
        self._after_initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self._active = False


@docs_group('Classes')
class AdaptivePlaywrightCrawler(
    Generic[TStaticCrawlingContext, TStaticParseResult, TStaticSelectResult],
    BasicCrawler[AdaptivePlaywrightCrawlingContext, AdaptivePlaywrightCrawlerStatisticState],
):
    """An adaptive web crawler capable of using both static HTTP request based crawling and browser based crawling.

    It uses a more limited crawling context interface so that it is able to switch to HTTP-only crawling when it detects
    that it may bring a performance benefit.
    It uses specific implementation of `AbstractHttpCrawler` and `PlaywrightCrawler`.

    ### Usage
    ```python
    from crawlee.crawlers import AdaptivePlaywrightCrawler, AdaptivePlaywrightCrawlingContext

    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        max_requests_per_crawl=5, playwright_crawler_specific_kwargs={'browser_type': 'chromium'}
    )

    @crawler.router.default_handler
    async def request_handler_for_label(context: AdaptivePlaywrightCrawlingContext) -> None:
        # Do some processing using `parsed_content`
        context.log.info(context.parsed_content.title)

        # Locate element h2 within 5 seconds
        h2 = await context.query_selector_one('h2', timedelta(milliseconds=5000))
        # Do stuff with element found by the selector
        context.log.info(h2)

        # Find more links and enqueue them.
        await context.enqueue_links()
        # Save some data.
        await context.push_data({'Visited url': context.request.url})

    await crawler.run(['https://crawlee.dev/'])
    ```
    """

    def __init__(
        self,
        *,
        static_parser: AbstractHttpParser[TStaticParseResult, TStaticSelectResult],
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[AdaptivePlaywrightCrawlerStatisticState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> None:
        """Initialize a new instance. Recommended way to create instance is to call factory methods.

        Recommended factory methods: `with_beautifulsoup_static_parser`, `with_parsel_static_parser`.

        Args:
            rendering_type_predictor: Object that implements RenderingTypePredictor and is capable of predicting which
                rendering method should be used. If None, then `DefaultRenderingTypePredictor` is used.
            result_checker: Function that evaluates whether crawling result is valid or not.
            result_comparator: Function that compares two crawling results and decides whether they are equivalent.
            static_parser: Implementation of `AbstractHttpParser`. Parser that will be used for static crawling.
            static_crawler_specific_kwargs: `AbstractHttpCrawler` only kwargs that are passed to the sub crawler.
            playwright_crawler_specific_kwargs: `PlaywrightCrawler` only kwargs that are passed to the sub crawler.
            statistics: A custom `Statistics[AdaptivePlaywrightCrawlerStatisticState]` instance, allowing the use of
                non-default configuration.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        # Some sub crawler kwargs are internally modified. Prepare copies.
        basic_crawler_kwargs_for_static_crawler = deepcopy(kwargs)
        basic_crawler_kwargs_for_pw_crawler = deepcopy(kwargs)

        # Adaptive crawling related.
        self.rendering_type_predictor = rendering_type_predictor or DefaultRenderingTypePredictor()
        self.result_checker = result_checker or (lambda _: True)
        self.result_comparator = result_comparator or create_default_comparator(result_checker)

        super().__init__(statistics=statistics, **kwargs)

        # Sub crawlers related.
        playwright_crawler_specific_kwargs = playwright_crawler_specific_kwargs or {}

        # Each sub crawler will use custom logger .
        static_logger = getLogger('Subcrawler_static')
        static_logger.setLevel(logging.ERROR)
        basic_crawler_kwargs_for_static_crawler['_logger'] = static_logger

        pw_logger = getLogger('Subcrawler_playwright')
        pw_logger.setLevel(logging.ERROR)
        basic_crawler_kwargs_for_pw_crawler['_logger'] = pw_logger

        # Initialize sub crawlers to create their pipelines.
        static_crawler_class = AbstractHttpCrawler.create_parsed_http_crawler_class(static_parser=static_parser)

        static_crawler = static_crawler_class(
            parser=static_parser,
            statistics=_NonPersistentStatistics(),
            **basic_crawler_kwargs_for_static_crawler,
        )
        playwright_crawler = PlaywrightCrawler(
            statistics=_NonPersistentStatistics(),
            **playwright_crawler_specific_kwargs,
            **basic_crawler_kwargs_for_pw_crawler,
        )

        # Register pre navigation hooks on sub crawlers
        self._pre_navigation_hooks = list[Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]]]()
        self._pre_navigation_hooks_pw_only = list[
            Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]]
        ]()

        async def adaptive_pre_navigation_hook_static(context: BasicCrawlingContext) -> None:
            for hook in self._pre_navigation_hooks:
                await hook(AdaptivePlaywrightPreNavCrawlingContext.from_pre_navigation_context(context))

        async def adaptive_pre_navigation_hook_pw(context: PlaywrightPreNavCrawlingContext) -> None:
            for hook in self._pre_navigation_hooks + self._pre_navigation_hooks_pw_only:
                await hook(AdaptivePlaywrightPreNavCrawlingContext.from_pre_navigation_context(context))

        static_crawler.pre_navigation_hook(adaptive_pre_navigation_hook_static)
        playwright_crawler.pre_navigation_hook(adaptive_pre_navigation_hook_pw)

        self._additional_context_managers = [
            *self._additional_context_managers,
            static_crawler.statistics,
            playwright_crawler.statistics,
            playwright_crawler._browser_pool,  # noqa: SLF001 # Intentional access to private member.
        ]

        # Sub crawler pipeline related
        self._pw_context_pipeline = playwright_crawler._context_pipeline  # noqa:SLF001  # Intentional access to private member.
        self._static_context_pipeline = static_crawler._context_pipeline  # noqa:SLF001  # Intentional access to private member.
        self._static_parser = static_parser

    @classmethod
    def with_beautifulsoup_static_parser(
        cls,
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        parser_type: BeautifulSoupParserType = 'lxml',
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[StatisticsState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup, Tag]:
        """Create `AdaptivePlaywrightCrawler` that uses `BeautifulSoup` for parsing static content."""
        if statistics is not None:
            adaptive_statistics = statistics.replace_state_model(AdaptivePlaywrightCrawlerStatisticState)
        else:
            adaptive_statistics = Statistics(state_model=AdaptivePlaywrightCrawlerStatisticState)
        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup, Tag](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=BeautifulSoupParser(parser=parser_type),
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            statistics=adaptive_statistics,
            **kwargs,
        )

    @classmethod
    def with_parsel_static_parser(
        cls,
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[StatisticsState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector, Selector]:
        """Create `AdaptivePlaywrightCrawler` that uses `Parcel` for parsing static content."""
        if statistics is not None:
            adaptive_statistics = statistics.replace_state_model(AdaptivePlaywrightCrawlerStatisticState)
        else:
            adaptive_statistics = Statistics(state_model=AdaptivePlaywrightCrawlerStatisticState)
        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector, Selector](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=ParselParser(),
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            statistics=adaptive_statistics,
            **kwargs,
        )

    async def _crawl_one(
        self,
        rendering_type: RenderingType,
        context: BasicCrawlingContext,
        state: dict[str, JsonSerializable] | None = None,
    ) -> SubCrawlerRun:
        """Perform a one request crawl with specific context pipeline and return `SubCrawlerRun`.

        `SubCrawlerRun` contains either result of the crawl or the exception that was thrown during the crawl.
        Sub crawler pipeline call is dynamically created based on the `rendering_type`.
        New copy-like context is created from passed `context` and `state` and is passed to sub crawler pipeline.
        """
        if state is not None:

            async def get_input_state(
                default_value: dict[str, JsonSerializable] | None = None,  # noqa:ARG001  # Intentionally unused arguments. Closure, that generates same output regardless of inputs.
            ) -> dict[str, JsonSerializable]:
                return state

            use_state_function = get_input_state
        else:
            use_state_function = context.use_state

        # New result is created and injected to newly created context. This is done to ensure isolation of sub crawlers.
        result = RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store)
        context_linked_to_result = BasicCrawlingContext(
            request=deepcopy(context.request),
            session=deepcopy(context.session),
            proxy_info=deepcopy(context.proxy_info),
            send_request=context.send_request,
            add_requests=result.add_requests,
            push_data=result.push_data,
            get_key_value_store=result.get_key_value_store,
            use_state=use_state_function,
            log=context.log,
        )

        try:
            await wait_for(
                lambda: self._pipeline_call_factory(
                    rendering_type=rendering_type, context_linked_to_result=context_linked_to_result
                ),
                timeout=self._request_handler_timeout,
                timeout_message=(
                    f'{rendering_type=!s} timed out after {self._request_handler_timeout.total_seconds()}seconds'
                ),
                logger=self._logger,
            )
            return SubCrawlerRun(result=result)
        except Exception as e:
            return SubCrawlerRun(exception=e)

    def _pipeline_call_factory(
        self, rendering_type: RenderingType, context_linked_to_result: BasicCrawlingContext
    ) -> Coroutine[Any, Any, None]:
        """Create sub crawler pipeline call."""
        if rendering_type == 'static':

            async def from_static_pipeline_to_top_router(
                context: ParsedHttpCrawlingContext[TStaticParseResult],
            ) -> None:
                adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_parsed_http_crawling_context(
                    context=context, parser=self._static_parser
                )
                await self.router(adaptive_crawling_context)

            return self._static_context_pipeline(context_linked_to_result, from_static_pipeline_to_top_router)

        if rendering_type == 'client only':

            async def from_pw_pipeline_to_top_router(context: PlaywrightCrawlingContext) -> None:
                adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                    context=context, parser=self._static_parser
                )
                await self.router(adaptive_crawling_context)

            return self._pw_context_pipeline(context_linked_to_result, from_pw_pipeline_to_top_router)

        raise RuntimeError(
            f'Not a valid rendering type. Must be one of the following: {", ".join(get_args(RenderingType))}'
        )

    @override
    async def _run_request_handler(self, context: BasicCrawlingContext) -> None:
        """Override BasicCrawler method that delegates request processing to sub crawlers.

        To decide which sub crawler should process the request it runs `rendering_type_predictor`.
        To check if results are valid it uses `result_checker`.
        To compare results of both sub crawlers it uses `result_comparator`.

        Reference implementation: https://github.com/apify/crawlee/blob/master/packages/playwright-crawler/src/internals/adaptive-playwright-crawler.ts
        """
        rendering_type_prediction = self.rendering_type_predictor.predict(context.request)
        should_detect_rendering_type = random() < rendering_type_prediction.detection_probability_recommendation

        if not should_detect_rendering_type:
            self.log.debug(
                f'Predicted rendering type {rendering_type_prediction.rendering_type} for {context.request.url}'
            )
            if rendering_type_prediction.rendering_type == 'static':
                context.log.debug(f'Running static request for {context.request.url}')
                self.track_http_only_request_handler_runs()

                static_run = await self._crawl_one(rendering_type='static', context=context)
                if static_run.result and self.result_checker(static_run.result):
                    self._context_result_map[context] = static_run.result
                    return
                if static_run.exception:
                    context.log.exception(
                        msg=f'Static crawler: failed for {context.request.url}', exc_info=static_run.exception
                    )
                else:
                    context.log.warning(f'Static crawler: returned a suspicious result for {context.request.url}')
                    self.track_rendering_type_mispredictions()

        context.log.debug(f'Running browser request handler for {context.request.url}')

        old_state_copy = None

        if should_detect_rendering_type:
            # Save copy of global state from `use_state` before it can be mutated by browser crawl.
            # This copy will be used in the static crawl to make sure they both run with same conditions and to
            # avoid static crawl to modify the state.
            # (This static crawl is performed only to evaluate rendering type detection.)
            kvs = await context.get_key_value_store()
            default_value = dict[str, JsonSerializable]()
            old_state: dict[str, JsonSerializable] = await kvs.get_value(self._CRAWLEE_STATE_KEY, default_value)
            old_state_copy = deepcopy(old_state)

        pw_run = await self._crawl_one('client only', context=context)
        self.track_browser_request_handler_runs()

        if pw_run.exception is not None:
            raise pw_run.exception

        if pw_run.result:
            self._context_result_map[context] = pw_run.result

            if should_detect_rendering_type:
                detection_result: RenderingType
                static_run = await self._crawl_one('static', context=context, state=old_state_copy)

                if static_run.result and self.result_comparator(static_run.result, pw_run.result):
                    detection_result = 'static'
                else:
                    detection_result = 'client only'

                context.log.debug(f'Detected rendering type {detection_result} for {context.request.url}')
                self.rendering_type_predictor.store_result(context.request, detection_result)

    def pre_navigation_hook(
        self,
        hook: Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]] | None = None,
        *,
        playwright_only: bool = False,
    ) -> Callable[[Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]]], None]:
        """Pre navigation hooks for adaptive crawler are delegated to sub crawlers.

        Optionally parametrized decorator.
        Hooks are wrapped in context that handles possibly missing `page` object by raising `AdaptiveContextError`.
        """

        def register_hooks(hook: Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]]) -> None:
            if playwright_only:
                self._pre_navigation_hooks_pw_only.append(hook)
            else:
                self._pre_navigation_hooks.append(hook)

        # No parameter in decorator. Execute directly.
        if hook:
            register_hooks(hook)

        # Return parametrized decorator that will be executed through decorator syntax if called with parameter.
        return register_hooks

    def track_http_only_request_handler_runs(self) -> None:
        self.statistics.state.http_only_request_handler_runs += 1

    def track_browser_request_handler_runs(self) -> None:
        self.statistics.state.browser_request_handler_runs += 1

    def track_rendering_type_mispredictions(self) -> None:
        self.statistics.state.rendering_type_mispredictions += 1


@dataclass(frozen=True)
class SubCrawlerRun:
    result: RequestHandlerRunResult | None = None
    exception: Exception | None = None
