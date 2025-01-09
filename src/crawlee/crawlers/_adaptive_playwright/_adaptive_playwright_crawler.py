from __future__ import annotations

import asyncio
import logging
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from random import random
from typing import TYPE_CHECKING, Any, Generic

from bs4 import BeautifulSoup
from parsel import Selector
from typing_extensions import Self, TypeVar

from crawlee._types import BasicCrawlingContext, JsonSerializable, RequestHandlerRunResult
from crawlee._utils.wait import wait_for
from crawlee.crawlers import (
    AbstractHttpCrawler,
    AbstractHttpParser,
    BasicCrawler,
    BeautifulSoupCrawlingContext,
    BeautifulSoupParserType,
    ContextPipeline,
    ParsedHttpCrawlingContext,
    ParselCrawlingContext,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatistics,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
    AdaptivePlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    DefaultRenderingTypePredictor,
    RenderingType,
    RenderingTypePredictor,
)
from crawlee.crawlers._adaptive_playwright._result_comparator import (
    SubCrawlerRun,
    create_comparator,
)
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
from crawlee.crawlers._parsel._parsel_parser import ParselParser
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Sequence
    from datetime import timedelta
    from types import TracebackType

    from typing_extensions import Unpack

    from crawlee import Request
    from crawlee.crawlers._abstract_http._abstract_http_crawler import _HttpCrawlerOptions
    from crawlee.crawlers._basic._basic_crawler import _BasicCrawlerOptions
    from crawlee.crawlers._playwright._playwright_crawler import PlaywrightCrawlerAdditionalOptions
    from crawlee.router import Router
    from crawlee.statistics import FinalStatistics


TStaticParseResult = TypeVar('TStaticParseResult')
TStaticCrawlingContext = TypeVar('TStaticCrawlingContext', bound=ParsedHttpCrawlingContext)


class _NoActiveStatistics(Statistics):
    """Statistics compliant object that is not supposed to do anything when active. To be used in sub crawlers."""

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

    pre_navigation_hook: Callable[[Callable[[PlaywrightPreNavCrawlingContext], Awaitable[None]]], None]
    pipeline: ContextPipeline[PlaywrightCrawlingContext]
    needed_contexts: list[AbstractAsyncContextManager]
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


@dataclass
class _OrphanStaticContextPipeline(Generic[TStaticCrawlingContext]):
    """Minimal setup required by static context pipeline to work without crawler."""

    pre_navigation_hook: Callable[[Callable[[BasicCrawlingContext], Awaitable[None]]], None]
    pipeline: ContextPipeline[TStaticCrawlingContext]
    needed_contexts: list[AbstractAsyncContextManager]
    top_router: Router[AdaptivePlaywrightCrawlingContext]

    def create_pipeline_call(self, top_context: BasicCrawlingContext) -> Coroutine[Any, Any, None]:
        """Call that will be used by the top crawler to run through the pipeline."""

        async def from_pipeline_to_top_router(context: TStaticCrawlingContext) -> None:
            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_parsed_http_crawling_context(context)
            await self.top_router(adaptive_crawling_context)

        return self.pipeline(top_context, from_pipeline_to_top_router)


class AdaptivePlaywrightCrawler(
    Generic[TStaticCrawlingContext, TStaticParseResult], BasicCrawler[AdaptivePlaywrightCrawlingContext]
):
    """Adaptive crawler that uses both specific implementation of `AbstractHttpCrawler` and `PlaywrightCrawler`.

    It tries to detect whether it is sufficient to crawl with `BeautifulSoupCrawler` (which is faster) or if
    `PlaywrightCrawler` should be used (in case `BeautifulSoupCrawler` did not work as expected for specific url.).

    # TODO: Add example
    """

    def __init__(
        self,
        *,
        static_parser: AbstractHttpParser[TStaticParseResult],
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        static_crawler_specific_kwargs: _HttpCrawlerOptions | None = None,
        playwright_crawler_specific_kwargs: PlaywrightCrawlerAdditionalOptions | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> None:
        """A default constructor.

        Args:
            rendering_type_predictor: Object that implements RenderingTypePredictor and is capable of predicting which
                rendering method should be used. If None, then `DefaultRenderingTypePredictor` is used.
            result_checker: Function that evaluates whether crawling result is valid or not.
            result_comparator: Function that compares two crawling results and decides whether they are equivalent.
            static_parser: Implementation of `AbstractHttpParser`. Parser that will be used for static crawling.
            static_crawler_specific_kwargs: `AbstractHttpCrawler` only kwargs that are passed to the sub crawler.
            playwright_crawler_specific_kwargs: `PlaywrightCrawler` only kwargs that are passed to the sub crawler.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        # Some sub crawler kwargs are internally modified. Prepare copies.
        basic_crawler_kwargs_for_static_crawler = deepcopy(kwargs)
        basic_crawler_kwargs_for_pw_crawler = deepcopy(kwargs)

        # Adaptive crawling related.
        self.rendering_type_predictor = rendering_type_predictor or DefaultRenderingTypePredictor()
        self.result_checker = result_checker or (lambda result: True)  #  noqa: ARG005  # Intentionally unused argument.
        self.result_comparator = result_comparator or create_comparator(result_checker)

        # Use AdaptivePlaywrightCrawlerStatistics.
        if 'statistics' in kwargs:
            # If statistics already specified by user, create AdaptivePlaywrightCrawlerStatistics from it.
            statistics = AdaptivePlaywrightCrawlerStatistics.from_statistics(statistics=kwargs['statistics'])
        else:
            statistics = AdaptivePlaywrightCrawlerStatistics()
        kwargs['statistics'] = statistics

        self.predictor_state = statistics.predictor_state

        super().__init__(**kwargs)

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

        # Each sub crawler will use own dummy statistics.
        basic_crawler_kwargs_for_static_crawler['statistics'] = _NoActiveStatistics()
        basic_crawler_kwargs_for_pw_crawler['statistics'] = _NoActiveStatistics()

        # Initialize sub crawlers to create their pipelines.
        static_crawler_class = AbstractHttpCrawler.create_parsed_http_crawler_class(static_parser=static_parser)

        static_crawler = static_crawler_class(
            parser=static_parser, **static_crawler_specific_kwargs, **basic_crawler_kwargs_for_static_crawler
        )
        playwright_crawler = PlaywrightCrawler(
            **playwright_crawler_specific_kwargs, **basic_crawler_kwargs_for_pw_crawler
        )

        required_contexts_pw_crawler: list[AbstractAsyncContextManager] = [
            playwright_crawler._statistics,  # noqa:SLF001  # Intentional access to private member.
            playwright_crawler._browser_pool,  # noqa:SLF001  # Intentional access to private member.
        ]
        required_contexts_static_crawler: list[AbstractAsyncContextManager] = [
            static_crawler._statistics,  # noqa:SLF001  # Intentional access to private member.
        ]

        self._pw_context_pipeline = _OrphanPlaywrightContextPipeline(
            pipeline=playwright_crawler._context_pipeline,  # noqa:SLF001  # Intentional access to private member.
            needed_contexts=required_contexts_pw_crawler,
            top_router=self.router,
            pre_navigation_hook=playwright_crawler.pre_navigation_hook,
            static_parser=static_parser,
        )
        self._static_context_pipeline = _OrphanStaticContextPipeline[ParsedHttpCrawlingContext[TStaticParseResult]](
            pipeline=static_crawler._context_pipeline,  # noqa:SLF001  # Intentional access to private member.
            needed_contexts=required_contexts_static_crawler,
            top_router=self.router,
            pre_navigation_hook=static_crawler.pre_navigation_hook,
        )

    @staticmethod
    def with_beautifulsoup_static_parser(
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        parser_type: BeautifulSoupParserType = 'lxml',
        static_crawler_specific_kwargs: _HttpCrawlerOptions[BeautifulSoupCrawlingContext] | None = None,
        playwright_crawler_specific_kwargs: PlaywrightCrawlerAdditionalOptions | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup]:
        """Creates `AdaptivePlaywrightCrawler` that uses `BeautifulSoup` for parsing static content."""
        parser_kwargs = {'parser': parser_type} if parser_type else {}

        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[BeautifulSoup], BeautifulSoup](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=BeautifulSoupParser(**parser_kwargs),
            static_crawler_specific_kwargs=static_crawler_specific_kwargs,
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            **kwargs,
        )

    @staticmethod
    def with_parsel_static_parser(
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        static_crawler_specific_kwargs: _HttpCrawlerOptions[ParselCrawlingContext] | None = None,
        playwright_crawler_specific_kwargs: PlaywrightCrawlerAdditionalOptions | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector]:
        """Creates `AdaptivePlaywrightCrawler` that uses `Parcel` for parsing static content."""
        return AdaptivePlaywrightCrawler[ParsedHttpCrawlingContext[Selector], Selector](
            rendering_type_predictor=rendering_type_predictor,
            result_checker=result_checker,
            result_comparator=result_comparator,
            static_parser=ParselParser(),
            static_crawler_specific_kwargs=static_crawler_specific_kwargs,
            playwright_crawler_specific_kwargs=playwright_crawler_specific_kwargs,
            **kwargs,
        )

    async def crawl_one_with(
        self,
        subcrawler_pipeline: _OrphanPlaywrightContextPipeline | _OrphanStaticContextPipeline,
        context: BasicCrawlingContext,
        timeout: timedelta,
        result: RequestHandlerRunResult,
        state: dict[str, JsonSerializable] | None = None,
    ) -> RequestHandlerRunResult:
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
            timeout=timeout,
            timeout_message=f'Sub crawler timed out after {timeout.total_seconds()} seconds',
            logger=self._logger,
        )
        return result

    async def run(
        self,
        requests: Sequence[str | Request] | None = None,
        *,
        purge_request_queue: bool = True,
    ) -> FinalStatistics:
        """Run the crawler until all requests are processed.

        Args:
            requests: The requests to be enqueued before the crawler starts.
            purge_request_queue: If this is `True` and the crawler is not being run for the first time, the default
                request queue will be purged.
        """
        contexts_to_enter = [
            cm
            for cm in self._static_context_pipeline.needed_contexts + self._pw_context_pipeline.needed_contexts
            if cm and getattr(cm, 'active', False) is False
        ]

        # Enter contexts required by sub crawler for them to be able to do `crawl_one`
        async with AsyncExitStack() as exit_stack:
            for context in contexts_to_enter:
                await exit_stack.enter_async_context(context)
            return await super().run(requests=requests, purge_request_queue=purge_request_queue)

        # AsyncExitStack can in theory swallow exceptions and so the return might not execute.
        # https://github.com/python/mypy/issues/7726
        raise RuntimeError('FinalStatistics not created.')

    # Can't use override as mypy does not like it for double underscore private method.
    async def _BasicCrawler__run_request_handler(self, context: BasicCrawlingContext) -> None:  # noqa: N802
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
                crawl_result = await self.crawl_one_with(
                    subcrawler_pipeline=subcrawler_pipeline,
                    context=context,
                    timeout=self._request_handler_timeout,
                    result=RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store),
                    state=use_state,
                )
                return SubCrawlerRun(result=crawl_result)
            except Exception as e:
                return SubCrawlerRun(exception=e)

        rendering_type_prediction = self.rendering_type_predictor.predict(context.request.url, context.request.label)
        should_detect_rendering_type = random() < rendering_type_prediction.detection_probability_recommendation

        if not should_detect_rendering_type:
            self.log.debug(
                f'Predicted rendering type {rendering_type_prediction.rendering_type} for {context.request.url}'
            )
            if rendering_type_prediction.rendering_type == 'static':
                context.log.debug(f'Running static request for {context.request.url}')
                self.predictor_state.track_http_only_request_handler_runs()

                static_run = await _run_subcrawler_pipeline(self._static_context_pipeline)
                if static_run.result and self.result_checker(static_run.result):
                    await self._commit_result(result=static_run.result, context=context)
                    return
                if static_run.exception:
                    context.log.exception(
                        msg=f'Static crawler: failed for {context.request.url}', exc_info=static_run.exception
                    )
                else:
                    context.log.warning(f'Static crawler: returned a suspicious result for {context.request.url}')
                    self.predictor_state.track_rendering_type_mispredictions()

        context.log.debug(f'Running browser request handler for {context.request.url}')

        if should_detect_rendering_type:
            # Save copy of global state from `use_state` before it can be mutated by browser crawl.
            # This copy will be used in the static crawl to make sure they both run with same conditions and to
            # avoid static crawl to modify the state.
            # (This static crawl is performed only to evaluate rendering type detection.)
            kvs = await context.get_key_value_store()
            default_value = dict[str, JsonSerializable]()
            old_state: dict[str, JsonSerializable] = await kvs.get_value(BasicCrawler.CRAWLEE_STATE_KEY, default_value)
            old_state_copy = deepcopy(old_state)

        pw_run = await _run_subcrawler_pipeline(self._pw_context_pipeline)
        self.predictor_state.track_browser_request_handler_runs()

        if pw_run.exception is not None:
            raise pw_run.exception

        if pw_run.result:
            await self._commit_result(result=pw_run.result, context=context)

            if should_detect_rendering_type:
                detection_result: RenderingType
                static_run = await _run_subcrawler_pipeline(self._static_context_pipeline, use_state=old_state_copy)

                if static_run.result and self.result_comparator(static_run.result, pw_run.result):
                    detection_result = 'static'
                else:
                    detection_result = 'client only'

                context.log.debug(f'Detected rendering type {detection_result} for {context.request.url}')
                self.rendering_type_predictor.store_result(context.request.url, context.request.label, detection_result)

    async def _commit_result(self, result: RequestHandlerRunResult, context: BasicCrawlingContext) -> None:
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

        def hook_with_wrapped_context(
            context: BasicCrawlingContext | PlaywrightPreNavCrawlingContext,
        ) -> Awaitable[None]:
            wrapped_context = AdaptivePlaywrightPreNavCrawlingContext.from_pre_navigation_contexts(context)
            return hook(wrapped_context)

        self._pw_context_pipeline.pre_navigation_hook(hook_with_wrapped_context)
        self._static_context_pipeline.pre_navigation_hook(hook_with_wrapped_context)
