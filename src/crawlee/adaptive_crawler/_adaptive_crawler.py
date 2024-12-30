from __future__ import annotations

import asyncio
import contextlib
from copy import deepcopy
from dataclasses import dataclass, field
from random import random
from typing import TYPE_CHECKING, Any, ContextManager, Callable

from typing_extensions import Self, TypeVar, Unpack, override, Never

from crawlee._types import BasicCrawlingContext
from crawlee.adaptive_crawler._crawl_type_predictor import CrawlType, CrawlTypePredictor
from crawlee.adaptive_crawler._result_handlers import (
    SubCrawlerResult,
    default_result_comparator, _PushDataKwargs, _AddRequestsKwargs,
)
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator, Sequence, AsyncIterator, Mapping

    from crawlee import Request
    from crawlee.statistics import FinalStatistics
    from crawlee.storages._request_provider import RequestProvider

TAdaptiveCrawlingContext = TypeVar('TAdaptiveCrawlingContext', bound=BasicCrawlingContext,
                                   default=BasicCrawlingContext)


class AdaptiveCrawler(BasicCrawler[TAdaptiveCrawlingContext]):

    @classmethod
    async def from_crawlers(cls,primary_crawler: BasicCrawler, secondary_crawler: BasicCrawler,
                            **crawler_kwargs: Unpack[BasicCrawlerOptions[TAdaptiveCrawlingContext]]) -> Self:
        return cls(primary_crawler=primary_crawler, secondary_crawler=secondary_crawler,
                   primary_request_provider=await primary_crawler.get_request_provider(),
                   secondary_request_provider=await secondary_crawler.get_request_provider(),
                   **crawler_kwargs)

    def __init__(
        self,
        # Could be abstracted in more than just two crawlers. Do not do it preemptively unless there is good use for it.
        primary_crawler: BasicCrawler,  # Preferred crawler, faster
        secondary_crawler: BasicCrawler,  # Slower back-up crawler
        primary_request_provider: RequestProvider,
        secondary_request_provider: RequestProvider,
        **kwargs: Unpack[BasicCrawlerOptions[TAdaptiveCrawlingContext]],
    ) -> None:
        # Sub crawlers related args:
        self._primary_crawler = primary_crawler
        self._secondary_crawler = secondary_crawler
        self._primary_request_provider = primary_request_provider
        self._secondary_request_provider = secondary_request_provider
        self.coordinator = _Coordinator(primary_crawler, secondary_crawler)

        # Dummy predictor for now.
        self.crawl_type_predictor = CrawlTypePredictor()

        # Result related args:
        self.result_comparator = default_result_comparator
        self.result_checker: Callable[[SubCrawlerResult], bool] = lambda result: True #  noqa: ARG005

        kwargs['_context_pipeline'] = ContextPipeline[TAdaptiveCrawlingContext]()
        super().__init__(**kwargs)


    @contextlib.contextmanager
    def _keep_alive_request_providers(self) -> Iterator[None]:
        """Set 'keep_alive' flag on request providers to prevent them from finishing when empty."""
        self._primary_request_provider.keep_alive = True
        self._secondary_request_provider.keep_alive = True
        yield
        self._primary_request_provider.keep_alive = False
        self._secondary_request_provider.keep_alive = False

    @contextlib.asynccontextmanager
    async def _clean_request_providers(self) -> AsyncIterator[None]:
        """Drop request providers of sub crawlers."""
        yield
        await self._primary_request_provider.drop()
        await self._secondary_request_provider.drop()

    async def run(self, requests: Sequence[str | Request] | None = None, *,
                  purge_request_queue: bool = True) -> FinalStatistics:
        """Run the adaptive crawler until all requests are processed.

        Start sub crawlers and top crawler.
        Sub crawlers are in stand-by by keeping their request providers alive even when empty.
        After top crawler 'run' method finishes, clean all resources of sub crawlers.

        Args:
            requests: The requests to be enqueued before the crawler starts.
            purge_request_queue: If this is `True` and the crawler is not being run for the first time, the default
                request queue will be purged.
        """
        async with self._clean_request_providers():
            with self._keep_alive_request_providers():
                tasks = [asyncio.create_task(self._primary_crawler.run()),
                         asyncio.create_task(self._secondary_crawler.run())]

                await super().run(requests=requests, purge_request_queue=purge_request_queue)

            await asyncio.gather(*tasks)

        # TODO: Handle stats
        return self._statistics.calculate()

    @override
    async def _BasicCrawler__run_request_handler(self, context: BasicCrawlingContext) -> None: # type: ignore[misc]  # Mypy does not understand name mangling override
        """Delegate to sub crawlers."""
        async def commit_result(result:SubCrawlerResult) -> None:
            """Perform push_data and add_requests on top crawler with arguments from sub crawler."""
            if result.add_request_kwargs:
                await context.add_requests(**result.add_request_kwargs)
            if result.push_data_kwargs:
                await context.push_data(**result.push_data_kwargs)
            # TODO: USE STATE


        should_run_primary_crawler = False

        crawl_type_prediction = self.crawl_type_predictor.predict(context.request.url, context.request.label)
        should_detect_crawl_type = random() < crawl_type_prediction.detection_probability_recommendation

        if not should_detect_crawl_type:
            self.log.debug(f'Predicted rendering type {crawl_type_prediction.crawl_type} for {context.request.url}')
            should_run_primary_crawler = crawl_type_prediction.crawl_type == 'primary'
        else:
            should_run_primary_crawler = True

        with self.coordinator.result_cleanup(request_id=context.request.id):

            if should_run_primary_crawler:
                await (self._primary_request_provider.add_request(deepcopy(context.request)))
                primary_crawler_result = await self.coordinator.get_result(
                    self._primary_crawler, context.request.id, timeout=self._request_handler_timeout.seconds)
                if primary_crawler_result.ok and self.result_checker(primary_crawler_result):
                    await commit_result(primary_crawler_result)
                    return
                if not primary_crawler_result.ok:
                    context.log.exception(msg=f'Primary crawler: {self._primary_crawler} failed for'
                                              f' {context.request.url}', exc_info=primary_crawler_result.exception)
                else:
                    context.log.warning(f'Primary crawler: : {self._primary_crawler} returned a suspicious result for'
                                        f' {context.request.url}')


            await (self._secondary_request_provider.add_request(deepcopy(context.request)))
            secondary_crawler_result = await self.coordinator.get_result(self._secondary_crawler, context.request.id,
                                                                         timeout=self._request_handler_timeout.seconds)

            if secondary_crawler_result.exception is not None:
                raise secondary_crawler_result.exception
            await commit_result(secondary_crawler_result)

            if should_detect_crawl_type:
                detection_result: CrawlType
                await (self._primary_request_provider.add_request(deepcopy(context.request)))
                primary_crawler_result = await self.coordinator.get_result(
                    self._primary_crawler, context.request.id, timeout=self._request_handler_timeout.seconds)

                if primary_crawler_result.ok and self.result_comparator(primary_crawler_result,
                                                                        secondary_crawler_result):
                    detection_result = 'primary'
                else:
                    detection_result = 'secondary'

                context.log.debug(f'Detected crawl type {detection_result} for {context.request.url}')
                self.crawl_type_predictor.store_result(context.request.url, context.request.label, detection_result)

            # TODO: Do some work with the result, compare, analyze, save them. Enhance context




class _Coordinator:
    """Class to share sub crawler results for specific request."""

    @dataclass
    class _AwaitableSubCrawlerResult:
        """Gradually created result of sub crawler. It is completed after finalize method is called."""
        push_data_kwargs: _PushDataKwargs | None = None
        add_request_kwargs: _AddRequestsKwargs | None = None
        state: Any | None = None
        exception: Exception | None = None
        _future: asyncio.Future = field(default_factory=asyncio.Future)

        def finalize(self) -> None:
            self._future.set_result(
                SubCrawlerResult(
                    push_data_kwargs=self.push_data_kwargs,
                    add_request_kwargs=self.add_request_kwargs,
                    state=self.state,
                    exception=self.exception
                ))

        def __await__(self) -> Generator[Any, None, SubCrawlerResult]:
            return self._future.__await__()


    def __init__(self, crawler1: BasicCrawler, crawler2: BasicCrawler) -> None:
        self._id1 = id(crawler1)
        self._id2 = id(crawler2)
        self._results: dict[int, dict[str, _Coordinator._AwaitableSubCrawlerResult]] = {self._id1: {}, self._id2: {}}

    def register_expected_result(self, request_id: str) -> None:
        """Create result entries for each sub crawler for specific request_id."""
        self._results[self._id1][request_id] = self._AwaitableSubCrawlerResult()
        self._results[self._id2][request_id] = self._AwaitableSubCrawlerResult()

    def finalize_result(self, crawler: BasicCrawler, request_id: str) -> None:
        """Finalize result of specific sub crawler for specific request_id. Such result is considered complete."""
        self._results[id(crawler)][request_id].finalize()

    async def get_result(self, crawler: BasicCrawler, request_id: str, timeout: float = 60.) -> SubCrawlerResult:
        """Get sub crawler result for specific request_id."""
        async with asyncio.timeout(timeout):
            return await self._results[id(crawler)][request_id]

    def set_push_data(self, crawler: BasicCrawler, request_id: str, push_data_kwargs: _PushDataKwargs) -> None:
        """Set 'push_data' related arguments to result of specific sub crawler for specific request_id."""
        self._results[id(crawler)][request_id].push_data_kwargs=push_data_kwargs

    def set_add_request(self, crawler: BasicCrawler, request_id: str, add_request_kwargs: _AddRequestsKwargs) -> None:
        """Set 'add_request' related arguments to result of specific sub crawler for specific request_id."""
        self._results[id(crawler)][request_id].add_request_kwargs = add_request_kwargs

    def set_exception(self, crawler: BasicCrawler, request_id: str, exception: Exception) -> None:
        """Set 'ok' value. Should be false if sub crawler failed to get results."""
        self._results[id(crawler)][request_id].exception = exception

    def remove_result(self, request_id: str) -> None:
        """Remove results of all sub crawlers for specific request_id."""
        if self._results[self._id1]:
            del self._results[self._id1][request_id]
        if self._results[self._id2]:
            del self._results[self._id2][request_id]

    def result_cleanup(self, request_id: str) -> ContextManager[None]:
        """Context for preparing and cleaning sub crawler results for specific request_id."""

        @contextlib.contextmanager
        def _result_cleanup() -> Iterator[None]:
                self.register_expected_result(request_id)
                yield
                self.remove_result(request_id)

        return _result_cleanup()



