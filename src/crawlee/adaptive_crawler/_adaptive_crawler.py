from __future__ import annotations
import asyncio
import contextlib
from asyncio import Future
from collections.abc import AsyncGenerator, Sequence

from copy import deepcopy
from dataclasses import fields, dataclass, field
from itertools import cycle
from logging import getLogger, exception
from typing import Any
from venv import logger

from bs4 import BeautifulSoup
from typing_extensions import Unpack, Self

from crawlee import Request
from crawlee._types import BasicCrawlingContext, JsonSerializable
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext
from crawlee.playwright_crawler import PlaywrightCrawlingContext
from crawlee.statistics import FinalStatistics
from crawlee.storages import RequestQueue
from crawlee.storages._request_provider import RequestProvider


# TODO: Think about generics later. Hardcode to BsCContext
class AdaptiveCrawlingContext(BeautifulSoupCrawlingContext, PlaywrightCrawlingContext):

    @classmethod
    def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext, soup: BeautifulSoup) -> Self:
        return cls(parsed_content=soup, **{field.name: getattr(context, field.name) for field in fields(context)})

    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: PlaywrightCrawlingContext, soup: BeautifulSoup) -> Self:
        return cls(response=None, infinite_scroll=None, page=None,
                   **{field.name: getattr(context, field.name) for field in fields(context)})




class AdaptiveCrawler(BasicCrawler):

    @classmethod
    async def from_crawlers(cls,primary_crawler: BasicCrawler, secondary_crawler: BasicCrawler, **crawler_kwargs):
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
        **kwargs: Unpack[BasicCrawlerOptions[BasicCrawlingContext]],
    ) -> None:
        self._primary_crawler = primary_crawler
        self._secondary_crawler = secondary_crawler
        self._primary_request_provider = primary_request_provider
        self._secondary_request_provider = secondary_request_provider

        # Compose the context pipeline.
        kwargs['_context_pipeline'] = ContextPipeline().compose(self._delegate_to_subcrawlers)
        self.coordinator = _Coordinator(primary_crawler, secondary_crawler)
        # This hack has to be done due to the fact, that various crawler have to share single global configuration and thus differnt _push_data functions can't be set from configuration
        super().__init__(**kwargs)

    async def _delegate_to_subcrawlers(self, context: BasicCrawlingContext) -> AsyncGenerator[
        BeautifulSoupCrawlingContext, None]:

        # This can be "atomic" with some sort of lock to prevent parallel delegations to prevent from crawler from resource blocking itself and to have consistent behavoir with JS
        # Or there can be limit on each subcrawler "active delegation count". Do only if needed.

        async with self.coordinator.result_cleanup(request_id=context.request.id):
            # TODO: some logic that decides when to run which crawler. Copy from JS
            # Each request has to be passed as copy to decouple processing, otherwise even if processed by different request queues it will be still coupled.
            await (self._primary_request_provider.add_request(deepcopy(context.request)))
            await (self._secondary_request_provider.add_request(deepcopy(context.request)))


            # This will always happen
            primary_crawler_result = await self.coordinator.get_result(self._primary_crawler, context.request.id)

            #This just sometimes
            secondary_crawler_result = await self.coordinator.get_result(self._secondary_crawler, context.request.id)
            # TODO: Do some work with the result, compare, analyze, save them. Enhance context

        # End of "atomic" lock

        yield context

    def _connect_crawlers(self):
        """Point storage of subcrawlers to the storage of top crawler or other related connections"""

        # Maybe not needed.

        pass

    @contextlib.contextmanager
    def _keep_alive_request_providers(self) -> None:
        self._primary_request_provider.keep_alive = True
        self._secondary_request_provider.keep_alive = True
        yield
        self._primary_request_provider.keep_alive = False
        self._secondary_request_provider.keep_alive = False

    @contextlib.asynccontextmanager
    async def _clean_request_providers(self) -> None:
        yield
        await self._primary_request_provider.drop()
        await self._secondary_request_provider.drop()

    async def run(self, requests: Sequence[str | Request] | None = None, *,
                  purge_request_queue: bool = True) -> FinalStatistics:
        # Delegate top crawler user's callback to subcrawlers

        async with self._clean_request_providers():
            with self._keep_alive_request_providers():
                tasks = [asyncio.create_task(self._primary_crawler.run()),
                         asyncio.create_task(self._secondary_crawler.run())]

                await super().run(requests=requests, purge_request_queue=purge_request_queue)

            await asyncio.gather(*tasks)

        # TODO: Handle stats

    async def _BasicCrawler__run_request_handler(self, context: BasicCrawlingContext) -> None:
        #TODO: this is uggly hack. Do nicely
        async def no_action(context: BasicCrawlingContext) -> None:
            # No need to run router handler methods again. It was already delegated to subcrawlers
            logger.info("No action")
            pass

        await self._context_pipeline(context, no_action)


class _Coordinator:
    """Some way to share state and results between different crawlers."""

    @dataclass(frozen=True)
    class SubCrawlerResult:
        push_data_kwargs: dict | None = None
        add_request_kwargs: dict | None = None
        links: list | None = None
        state: any | None = None

    @dataclass
    class _AwaitableSubCrawlerResult():
        # Could be done by subclassing Future instead.
        push_data_kwargs: dict | None = None
        add_request_kwargs: dict | None = None
        links: list | None = None
        state: any | None = None
        _future: Future = field(default_factory=Future)

        def finalize(self):
            self._future.set_result(
                _Coordinator.SubCrawlerResult(
                    push_data_kwargs=self.push_data_kwargs,
                    add_request_kwargs=self.add_request_kwargs,
                    links=self.links,
                    state=self.state,
                ))

        def __await__(self):
            return self._future.__await__()





    def __init__(self, crawler1: BasicCrawler, crawler2: BasicCrawler) -> None:
        self._id1 = id(crawler1)
        self._id2 = id(crawler2)
        self._results: dict[str, dict[str, _Coordinator._AwaitableSubCrawlerResult]] = {self._id1: {}, self._id2: {}}
        self.locks = {}

    def register_expected_result(self, request_id: str):
        self._results[self._id1][request_id] = self._AwaitableSubCrawlerResult()
        self._results[self._id2][request_id] = self._AwaitableSubCrawlerResult()

    def finalize_result(self, crawler: BasicCrawler, request_id: str):
        self._results[id(crawler)][request_id].finalize()

    async def get_result(self, crawler: BasicCrawler, request_id: str, timeout: int = 10) -> _Coordinator.SubCrawlerResult:
        async with asyncio.timeout(timeout):
            # TODO: Handle timeouts and edge cases
            return await self._results[id(crawler)][request_id]

    def set_push_data(self, crawler: BasicCrawler, request_id: str, push_data_kwargs: dict) -> None:
        self._results[id(crawler)][request_id].push_data_kwargs=push_data_kwargs

    def set_add_request(self, crawler: BasicCrawler, request_id: str, add_request_kwargs: dict):
        self._results[id(crawler)][request_id].add_request_kwargs = add_request_kwargs

    def remove_result(self, request_id):
        if self._results[self._id1]:
            del self._results[self._id1][request_id]
        if self._results[self._id2]:
            del self._results[self._id2][request_id]

    def result_cleanup(self, request_id: str):
        class _ResultCleanup:
            async def __aenter__(_):
                self.register_expected_result(request_id)

            async def __aexit__(_, exc_type, exc_val, exc_tb):
                self.remove_result(request_id)
        return _ResultCleanup()



