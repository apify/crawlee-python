from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from random import random
from typing import TYPE_CHECKING, Any, Generic

from bs4 import BeautifulSoup
from parsel import Selector
from typing_extensions import Self, TypeVar, override

from crawlee._types import BasicCrawlingContext, JsonSerializable, RequestHandlerRunResult
from crawlee._utils.wait import wait_for
from crawlee.crawlers import (
    AbstractHttpCrawler,
    AbstractHttpParser,
    BasicCrawler,
    BeautifulSoupParserType,
    ContextPipeline,
    ParsedHttpCrawlingContext,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatisticState,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    RandomRenderingTypePredictor as DefaultRenderingTypePredictor,
)
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    RenderingType,
    RenderingTypePredictor,
)
from crawlee.crawlers._adaptive_playwright._result_comparator import (
    SubCrawlerRun,
    create_default_comparator,
)
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
from crawlee.crawlers._parsel._parsel_parser import ParselParser
from crawlee.statistics import Statistics, StatisticsState

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from types import TracebackType

    from typing_extensions import Unpack

    from crawlee.crawlers._abstract_http._abstract_http_crawler import (
        _HttpCrawlerAdditionalOptions,
    )
    from crawlee.crawlers._basic._basic_crawler import _BasicCrawlerOptions
    from crawlee.crawlers._playwright._playwright_crawler import _PlaywrightCrawlerAdditionalOptions
    from crawlee.router import Router


TStaticParseResult = TypeVar('TStaticParseResult')
TStaticCrawlingContext = TypeVar('TStaticCrawlingContext', bound=ParsedHttpCrawlingContext)


class _NoActiveStatistics(Statistics):
    """Statistics compliant object that is not supposed to do anything when active. To be used in sub crawlers."""

    def __init__(self) -> None:
        super().__init__(state_model=StatisticsState)
        self._active = True

    async def __aenter__(self) -> Self:
        self._active = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self._active = False


@dataclass
class _OrphanPlaywrightContextPipeline(Generic[TStaticParseResult]):
    """Minimal setup required by playwright context pipeline to work without crawler."""

    pipeline: ContextPipeline[PlaywrightCrawlingContext]
    top_router: Router[AdaptivePlaywrightCrawlingContext]
    static_parser: AbstractHttpParser[TStaticParseResult]

    def create_pipeline_call(self, top_context: BasicCrawlingContext) -> Coroutine[Any, Any, None]:
        """Call that will be used by the top crawler to run through the pipeline."""

        async def from_pipeline_to_top_router(context: PlaywrightCrawlingContext) -> None:
            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                context=context, parser=self.static_parser
            )
            await self.top_router(adaptive_crawling_context)

        return self.pipeline(top_context, from_pipeline_to_top_router)

    def __str__(self) -> str:
        return 'Playwright context pipeline'


@dataclass
class _OrphanStaticContextPipeline(Generic[TStaticCrawlingContext]):
    """Minimal setup required by static context pipeline to work without crawler."""

    pipeline: ContextPipeline[TStaticCrawlingContext]
    top_router: Router[AdaptivePlaywrightCrawlingContext]

    def create_pipeline_call(self, top_context: BasicCrawlingContext) -> Coroutine[Any, Any, None]:
        """Call that will be used by the top crawler to run through the pipeline."""

        async def from_pipeline_to_top_router(context: TStaticCrawlingContext) -> None:
            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_parsed_http_crawling_context(context)
            await self.top_router(adaptive_crawling_context)

        return self.pipeline(top_context, from_pipeline_to_top_router)

    def __str__(self) -> str:
        return 'Static context pipeline'


class AdaptivePlaywrightCrawler(
    Generic[TStaticCrawlingContext, TStaticParseResult],
    BasicCrawler[AdaptivePlaywrightCrawlingContext, AdaptivePlaywrightCrawlerStatisticState],
):
    """Adaptive crawler that uses both specific implementation of `AbstractHttpCrawler` and `PlaywrightCrawler`.

    It tries to detect whether it is sufficient to crawl without browser (which is faster) or if
    `PlaywrightCrawler` should be used (in case previous method did not work as expected for specific url.).

    # TODO: Add example
    """

    def __init__(
        self,
        *,
        static_parser: AbstractHttpParser[TStaticParseResult],
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        static_crawler_specific_kwargs: _HttpCrawlerAdditionalOptions | None = None,
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[AdaptivePlaywrightCrawlerStatisticState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> None:
        """A default constructor. Recommended way to create instance is to call factory methods `with_*_static_parser`.

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
        self.result_checker = result_checker or (lambda result: True)  #  noqa: ARG005  # Intentionally unused argument.
        self.result_comparator = result_comparator or create_default_comparator(result_checker)

        super().__init__(statistics=statistics, **kwargs)

        # Sub crawlers related.
        playwright_crawler_specific_kwargs = playwright_crawler_specific_kwargs or {}
        static_crawler_specific_kwargs = static_crawler_specific_kwargs or {}

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
            statistics=_NoActiveStatistics(),
            **static_crawler_specific_kwargs,
            **basic_crawler_kwargs_for_static_crawler,
        )
        playwright_crawler = PlaywrightCrawler(
            statistics=_NoActiveStatistics(),
            **playwright_crawler_specific_kwargs,
            **basic_crawler_kwargs_for_pw_crawler,
        )

        self._pre_navigation_hooks = list[Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]]]()

        async def adaptive_pre_navigation_hook(context: BasicCrawlingContext | PlaywrightPreNavCrawlingContext) -> None:
            for hook in self._pre_navigation_hooks:
                await hook(AdaptivePlaywrightPreNavCrawlingContext.from_pre_navigation_contexts(context))

        playwright_crawler.pre_navigation_hook(adaptive_pre_navigation_hook)
        static_crawler.pre_navigation_hook(adaptive_pre_navigation_hook)

        self._additional_context_managers = [*self._additional_context_managers, playwright_crawler._browser_pool]  # noqa: SLF001 # Intentional access to private member.

        self._pw_context_pipeline = _OrphanPlaywrightContextPipeline(
            pipeline=playwright_crawler._context_pipeline,  # noqa:SLF001  # Intentional access to private member.
            top_router=self.router,
            static_parser=static_parser,
        )
        self._static_context_pipeline = _OrphanStaticContextPipeline[ParsedHttpCrawlingContext[TStaticParseResult]](
            pipeline=static_crawler._context_pipeline,  # noqa:SLF001  # Intentional access to private member.
            top_router=self.router,
        )

    @staticmethod
    def with_beautifulsoup_static_parser(
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        parser_type: BeautifulSoupParserType = 'lxml',
        static_crawler_specific_kwargs: _HttpCrawlerAdditionalOptions | None = None,
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[StatisticsState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup]:
        """Creates `AdaptivePlaywrightCrawler` that uses `BeautifulSoup` for parsing static content."""
        if statistics is not None:
            adaptive_statistics = statistics.replace_state_model(AdaptivePlaywrightCrawlerStatisticState)
        else:
            adaptive_statistics = Statistics(state_model=AdaptivePlaywrightCrawlerStatisticState)
        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=BeautifulSoupParser(parser=parser_type),
            static_crawler_specific_kwargs=static_crawler_specific_kwargs,
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            statistics=adaptive_statistics,
            **kwargs,
        )

    @staticmethod
    def with_parsel_static_parser(
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        static_crawler_specific_kwargs: _HttpCrawlerAdditionalOptions | None = None,
        playwright_crawler_specific_kwargs: _PlaywrightCrawlerAdditionalOptions | None = None,
        statistics: Statistics[StatisticsState] | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector]:
        """Creates `AdaptivePlaywrightCrawler` that uses `Parcel` for parsing static content."""
        if statistics is not None:
            adaptive_statistics = statistics.replace_state_model(AdaptivePlaywrightCrawlerStatisticState)
        else:
            adaptive_statistics = Statistics(state_model=AdaptivePlaywrightCrawlerStatisticState)
        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=ParselParser(),
            static_crawler_specific_kwargs=static_crawler_specific_kwargs,
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            statistics=adaptive_statistics,
            **kwargs,
        )

    async def _crawl_one_with(
        self,
        subcrawler_pipeline: _OrphanPlaywrightContextPipeline | _OrphanStaticContextPipeline,
        context: BasicCrawlingContext,
        result: RequestHandlerRunResult,
        state: dict[str, JsonSerializable] | None = None,
    ) -> RequestHandlerRunResult:
        """Perform a one request crawl with specific context pipeline and return result of this crawl.

        Use `context`, `result` and `state` to create new copy-like context that is passed to the `subcrawler_pipeline`.
        """
        if state is not None:

            async def get_input_state(
                default_value: dict[str, JsonSerializable] | None = None,  # noqa:ARG001  # Intentionally unused arguments. Closure, that generates same output regardless of inputs.
            ) -> dict[str, JsonSerializable]:
                return state

            use_state_function = get_input_state
        else:
            use_state_function = context.use_state

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

        await wait_for(
            lambda: subcrawler_pipeline.create_pipeline_call(context_linked_to_result),
            timeout=self._request_handler_timeout,
            timeout_message=(
                f'{subcrawler_pipeline=!s} timed out after {self._request_handler_timeout.total_seconds()}seconds'
            ),
            logger=self._logger,
        )
        return result

    @override
    async def _run_request_handler(self, context: BasicCrawlingContext) -> None:
        """Override BasicCrawler method that delegates request processing to sub crawlers.

        To decide which sub crawler should process the request it runs `rendering_type_predictor`.
        To check if results are valid it uses `result_checker`.
        To compare results of both sub crawlers it uses `result_comparator`.

        Reference implementation: https://github.com/apify/crawlee/blob/master/packages/playwright-crawler/src/internals/adaptive-playwright-crawler.ts
        """

        async def _run_subcrawler_pipeline(
            subcrawler_pipeline: _OrphanPlaywrightContextPipeline | _OrphanStaticContextPipeline,
            use_state: dict | None = None,
        ) -> SubCrawlerRun:
            """Helper closure that creates new `RequestHandlerRunResult` and delegates request handling to sub crawler.

            Produces `SubCrawlerRun` that either contains filled `RequestHandlerRunResult` or exception.
            """
            try:
                crawl_result = await self._crawl_one_with(
                    subcrawler_pipeline=subcrawler_pipeline,
                    context=context,
                    result=RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store),
                    state=use_state,
                )
                return SubCrawlerRun(result=crawl_result)
            except Exception as e:
                return SubCrawlerRun(exception=e)

        rendering_type_prediction = self.rendering_type_predictor.predict(context.request)
        should_detect_rendering_type = random() < rendering_type_prediction.detection_probability_recommendation

        if not should_detect_rendering_type:
            self.log.debug(
                f'Predicted rendering type {rendering_type_prediction.rendering_type} for {context.request.url}'
            )
            if rendering_type_prediction.rendering_type == 'static':
                context.log.debug(f'Running static request for {context.request.url}')
                self.track_http_only_request_handler_runs()

                static_run = await _run_subcrawler_pipeline(self._static_context_pipeline)
                if static_run.result and self.result_checker(static_run.result):
                    await self._push_result_to_context(result=static_run.result, context=context)
                    return
                if static_run.exception:
                    context.log.exception(
                        msg=f'Static crawler: failed for {context.request.url}', exc_info=static_run.exception
                    )
                else:
                    context.log.warning(f'Static crawler: returned a suspicious result for {context.request.url}')
                    self.track_rendering_type_mispredictions()

        context.log.debug(f'Running browser request handler for {context.request.url}')

        if should_detect_rendering_type:
            # Save copy of global state from `use_state` before it can be mutated by browser crawl.
            # This copy will be used in the static crawl to make sure they both run with same conditions and to
            # avoid static crawl to modify the state.
            # (This static crawl is performed only to evaluate rendering type detection.)
            kvs = await context.get_key_value_store()
            default_value = dict[str, JsonSerializable]()
            old_state: dict[str, JsonSerializable] = await kvs.get_value(self._CRAWLEE_STATE_KEY, default_value)
            old_state_copy = deepcopy(old_state)

        pw_run = await _run_subcrawler_pipeline(self._pw_context_pipeline)
        self.track_browser_request_handler_runs()

        if pw_run.exception is not None:
            raise pw_run.exception

        if pw_run.result:
            await self._push_result_to_context(result=pw_run.result, context=context)

            if should_detect_rendering_type:
                detection_result: RenderingType
                static_run = await _run_subcrawler_pipeline(self._static_context_pipeline, use_state=old_state_copy)

                if static_run.result and self.result_comparator(static_run.result, pw_run.result):
                    detection_result = 'static'
                else:
                    detection_result = 'client only'

                context.log.debug(f'Detected rendering type {detection_result} for {context.request.url}')
                self.rendering_type_predictor.store_result(context.request, detection_result)

    async def _push_result_to_context(self, result: RequestHandlerRunResult, context: BasicCrawlingContext) -> None:
        """Execute calls from `result` on the context."""
        result_tasks = (
            [asyncio.create_task(context.push_data(**kwargs)) for kwargs in result.push_data_calls]
            + [asyncio.create_task(context.add_requests(**kwargs)) for kwargs in result.add_requests_calls]
            + [asyncio.create_task(self._commit_key_value_store_changes(result, context.get_key_value_store))]
        )

        await asyncio.gather(*result_tasks)

    def pre_navigation_hook(
        self,
        hook: Callable[[AdaptivePlaywrightPreNavCrawlingContext], Awaitable[None]],
    ) -> None:
        """Pre navigation hooks for adaptive crawler are delegated to sub crawlers.

        Hooks are wrapped in context that handles possibly missing `page` object by throwing `AdaptiveContextError`.
        Hooks that try to access `context.page` will have to catch this exception if triggered by static pipeline.
        """
        self._pre_navigation_hooks.append(hook)

    def track_http_only_request_handler_runs(self) -> None:
        self.statistics.state.http_only_request_handler_runs += 1

    def track_browser_request_handler_runs(self) -> None:
        self.statistics.state.browser_request_handler_runs += 1

    def track_rendering_type_mispredictions(self) -> None:
        self.statistics.state.rendering_type_mispredictions += 1
