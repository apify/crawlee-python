from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from copy import deepcopy
from logging import getLogger
from random import random
from typing import TYPE_CHECKING, Any

from IPython.core.completer import TypedDict

from crawlee._types import BasicCrawlingContext, JsonSerializable, RequestHandlerRunResult
from crawlee._utils.docs import docs_group
from crawlee.crawlers import (
    BasicCrawler,
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
    BeautifulSoupParserType,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._abstract_http._abstract_http_crawler import _HttpCrawlerOptions
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler_statistics import (
    AdaptivePlaywrightCrawlerStatistics,
)
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawling_context import (
    AdaptivePlaywrightCrawlingContext,
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
from crawlee.statistics import Statistics

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from typing_extensions import NotRequired, Unpack

    from crawlee import Request
    from crawlee.browsers import BrowserPool
    from crawlee.browsers._types import BrowserType
    from crawlee.crawlers._basic._basic_crawler import _BasicCrawlerOptions
    from crawlee.statistics import FinalStatistics


@docs_group('Data structures')
class _BeautifulsoupCrawlerAdditionalOptions(_HttpCrawlerOptions):
    """Additional options that can be specified for BeautifulsoupCrawler."""

    parser: NotRequired[BeautifulSoupParserType]
    """Parser type used by BeautifulSoup."""


@docs_group('Data structures')
class _PlaywrightCrawlerAdditionalOptions(TypedDict):
    """Additional options that can be specified for PlaywrightCrawler."""

    browser_pool: NotRequired[BrowserPool]
    """A `BrowserPool` instance to be used for launching the browsers and getting pages."""

    browser_type: NotRequired[BrowserType]
    """The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided."""

    browser_launch_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
                documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
                This option should not be used if `browser_pool` is provided."""

    browser_new_context_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                Playwright documentation: https://playwright.dev/python/docs/api/class-browser#browser-new-context.
                This option should not be used if `browser_pool` is provided."""

    headless: NotRequired[bool]
    """Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided."""


class AdaptivePlaywrightCrawler(BasicCrawler[AdaptivePlaywrightCrawlingContext]):
    """Adaptive crawler that uses both `BeautifulSoupCrawler` and `PlaywrightCrawler`.

    It tries to detect whether it is sufficient to crawl with `BeautifulSoupCrawler` (which is faster) or if
    `PlaywrightCrawler` should be used (in case `BeautifulSoupCrawler` did not work as expected for specific url.).

    # TODO: Add example
    """

    def __init__(
        self,
        rendering_type_predictor: RenderingTypePredictor | None = None,
        result_checker: Callable[[RequestHandlerRunResult], bool] | None = None,
        result_comparator: Callable[[RequestHandlerRunResult, RequestHandlerRunResult], bool] | None = None,
        beautifulsoup_crawler_kwargs: _BeautifulsoupCrawlerAdditionalOptions | None = None,
        playwright_crawler_args: _PlaywrightCrawlerAdditionalOptions | None = None,
        **kwargs: Unpack[_BasicCrawlerOptions],
    ) -> None:
        """A default constructor.

        Args:
            rendering_type_predictor: Object that implements RenderingTypePredictor and is capable of predicting which
                rendering method should be used. If None, then `DefaultRenderingTypePredictor` is used.
            result_checker: Function that evaluates whether crawling result is valid or not.
            result_comparator: Function that compares two crawling results and decides whether they are equivalent.
            beautifulsoup_crawler_kwargs: BeautifulsoupCrawler only kwargs that are passed to the sub crawler.
            playwright_crawler_args: PlaywrightCrawler only kwargs that are passed to the sub crawler.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        # Some sub crawler kwargs are internally modified. Prepare copies.
        bs_kwargs = deepcopy(kwargs)
        pw_kwargs = deepcopy(kwargs)

        # Adaptive crawling related.
        self.rendering_type_predictor = rendering_type_predictor or DefaultRenderingTypePredictor()
        self.result_checker = result_checker or (lambda result: True)  #  noqa: ARG005  # Intentionally unused argument.

        self.result_comparator = result_comparator or create_comparator(result_checker)

        # Use AdaptivePlaywrightCrawlerStatistics.
        # Very hard to work with current "fake generic" Statistics. TODO: Discuss best approach.
        if 'statistics' in kwargs:
            # If statistics already specified by user, create AdaptivePlaywrightCrawlerStatistics from it.
            statistics = AdaptivePlaywrightCrawlerStatistics.from_statistics(statistics=kwargs['statistics'])
        else:
            statistics = AdaptivePlaywrightCrawlerStatistics()
        kwargs['statistics'] = statistics

        # self.statistics is hard coded in BasicCrawler to Statistics, so even when we save children class in it, mypy
        # will complain about using child-specific methods. Save same object to another attribute so that
        # AdaptivePlaywrightCrawlerStatistics specific methods can be access in "type safe manner".
        self.adaptive_statistics = statistics

        # Sub crawlers related.
        beautifulsoup_crawler_kwargs = beautifulsoup_crawler_kwargs or {}
        beautifulsoup_crawler_kwargs.setdefault('parser', 'lxml')
        playwright_crawler_args = playwright_crawler_args or {}

        # Each sub crawler will use custom logger .
        bs_logger = getLogger('Subcrawler_BS')
        bs_logger.setLevel(logging.ERROR)
        bs_kwargs['_logger'] = bs_logger

        pw_logger = getLogger('Subcrawler_PW')
        pw_logger.setLevel(logging.ERROR)
        pw_kwargs['_logger'] = pw_logger

        # Each sub crawler will use own statistics.
        bs_kwargs['statistics'] = Statistics(periodic_message_logger=bs_logger)
        pw_kwargs['statistics'] = Statistics(periodic_message_logger=pw_logger)

        self.beautifulsoup_crawler = BeautifulSoupCrawler(**beautifulsoup_crawler_kwargs, **bs_kwargs)
        self.playwright_crawler = PlaywrightCrawler(**playwright_crawler_args, **pw_kwargs)

        @self.beautifulsoup_crawler.router.default_handler
        async def request_handler_beautiful_soup(context: BeautifulSoupCrawlingContext) -> None:
            """Handler for routing from beautifulsoup_crawler to adaptive_crawler handler."""
            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_beautifulsoup_crawling_context(context)
            await self.router(adaptive_crawling_context)

        @self.playwright_crawler.router.default_handler
        async def request_handler_playwright(context: PlaywrightCrawlingContext) -> None:
            """Handler for routing from playwright_crawler to adaptive_crawler handler."""
            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                context=context, beautiful_soup_parser_type=beautifulsoup_crawler_kwargs['parser']
            )
            await self.router(adaptive_crawling_context)

        super().__init__(**kwargs)

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
            for cm in (
                self.beautifulsoup_crawler.crawl_one_required_contexts
                + self.playwright_crawler.crawl_one_required_contexts
            )
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

        async def _run_subcrawler(
            crawler: BeautifulSoupCrawler | PlaywrightCrawler, use_state: dict | None = None
        ) -> SubCrawlerRun:
            """Helper closure that creates new `RequestHandlerRunResult` and delegates request handling to sub crawler.

            Produces `SubCrawlerRun` that either contains filled `RequestHandlerRunResult` or exception.
            """
            try:
                crawl_result = await crawler.crawl_one(
                    context=context,
                    request_handler_timeout=self._request_handler_timeout,
                    result=RequestHandlerRunResult(key_value_store_getter=self.get_key_value_store),
                    use_state=use_state,
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
                self.adaptive_statistics.track_http_only_request_handler_runs()

                bs_run = await _run_subcrawler(self.beautifulsoup_crawler)
                if bs_run.result and self.result_checker(bs_run.result):
                    await self.commit_result(result=bs_run.result, context=context)
                    return
                if bs_run.exception:
                    context.log.exception(
                        msg=f'Static crawler: failed for {context.request.url}', exc_info=bs_run.exception
                    )
                else:
                    context.log.warning(f'Static crawler: returned a suspicious result for {context.request.url}')
                    self.adaptive_statistics.track_rendering_type_mispredictions()

        context.log.debug(f'Running browser request handler for {context.request.url}')

        kvs = await context.get_key_value_store()
        default_value = dict[str, JsonSerializable]()
        old_state: dict[str, JsonSerializable] = await kvs.get_value(BasicCrawler.CRAWLEE_STATE_KEY, default_value)
        old_state_copy = deepcopy(old_state)

        pw_run = await _run_subcrawler(self.playwright_crawler)
        self.adaptive_statistics.track_browser_request_handler_runs()

        if pw_run.exception is not None:
            raise pw_run.exception

        if pw_run.result:
            await self.commit_result(result=pw_run.result, context=context)

            if should_detect_rendering_type:
                detection_result: RenderingType
                bs_run = await _run_subcrawler(self.beautifulsoup_crawler, use_state=old_state_copy)

                if bs_run.result and self.result_comparator(bs_run.result, pw_run.result):
                    detection_result = 'static'
                else:
                    detection_result = 'client only'

                context.log.debug(f'Detected rendering type {detection_result} for {context.request.url}')
                self.rendering_type_predictor.store_result(context.request.url, context.request.label, detection_result)

    async def commit_result(self, result: RequestHandlerRunResult, context: BasicCrawlingContext) -> None:
        result_tasks = (
            [asyncio.create_task(context.push_data(**kwargs)) for kwargs in result.push_data_calls]
            + [asyncio.create_task(context.add_requests(**kwargs)) for kwargs in result.add_requests_calls]
            + [asyncio.create_task(self._commit_key_value_store_changes(result))]
        )

        await asyncio.gather(*result_tasks)

    def pre_navigation_hook(self, hook: Callable[[Any], Awaitable[None]]) -> None:
        """Pre navigation hooks for adaptive crawler are delegated to sub crawlers."""
        raise RuntimeError(
            'Pre navigation hooks are ambiguous in adaptive crawling context. Use specific hook instead:'
            '`pre_navigation_hook_pw` for playwright sub crawler related hooks or'
            '`pre_navigation_hook_bs`for beautifulsoup sub crawler related hooks. \n'
            f'{hook=} will not be used!!!'
        )

    def pre_navigation_hook_pw(self, hook: Callable[[PlaywrightPreNavCrawlingContext], Awaitable[None]]) -> None:
        """Pre navigation hooks for playwright sub crawler of adaptive crawler."""
        self.playwright_crawler.pre_navigation_hook(hook)

    def pre_navigation_hook_bs(self, hook: Callable[[BasicCrawlingContext], Awaitable[None]]) -> None:
        """Pre navigation hooks for beautifulsoup sub crawler of adaptive crawler."""
        self.beautifulsoup_crawler.pre_navigation_hook(hook)
