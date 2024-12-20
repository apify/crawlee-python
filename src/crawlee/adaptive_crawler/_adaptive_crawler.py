import asyncio
from asyncio import Future
from collections.abc import AsyncGenerator, Sequence
from copy import deepcopy
from dataclasses import fields
from itertools import cycle
from logging import getLogger, exception
from typing import Any

from bs4 import BeautifulSoup
from typing_extensions import Unpack, Self

from crawlee import Request
from crawlee._types import BasicCrawlingContext
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext
from crawlee.playwright_crawler import PlaywrightCrawlingContext
from crawlee.statistics import FinalStatistics
from crawlee.storages import RequestQueue

class AdaptiveCrawlingContext(BeautifulSoupCrawlingContext, PlaywrightCrawlingContext):

    @classmethod
    def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext, soup: BeautifulSoup) -> Self:
        return cls(parsed_content=soup, **{field.name: getattr(context, field.name) for field in fields(context)})

    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: PlaywrightCrawlingContext, soup: BeautifulSoup) -> Self:
        return cls(response=None, infinite_scroll=None, page=None, **{field.name: getattr(context, field.name) for field in fields(context)})

class AdaptiveCrawler(BasicCrawler):
    def __init__(
        self,
        # Could be abstracted in more than just two crawlers. Do not do it preemptively unless there is good use for it.
        primary_crawler: BasicCrawler,  # Preferred crawler
        secondary_crawler: BasicCrawler,
        **kwargs: Unpack[BasicCrawlerOptions[BasicCrawlingContext]],
    ) -> None:
        self._primary_crawler = primary_crawler
        self._secondary_crawler = secondary_crawler
        # Compose the context pipeline.
        kwargs['_context_pipeline'] = ContextPipeline().compose(self._delegate_to_subcrawlers)
        self.coordinator = _Coordinator(primary_crawler, secondary_crawler)
        super().__init__(**kwargs)


    async def _delegate_to_subcrawlers(self, context: BasicCrawlingContext) -> AsyncGenerator[AdaptiveCrawlingContext, None]:

        # This can be "atomic" with some sort of lock to prevent parallel delegations to prevent from crawler from resource blocking itself and to have consistent behavoir with JS
        # Or there can be limit on each subcrawler "active delegation count". Do only if needed.


        async with self.coordinator.result_cleanup(request_id=context.request.id):
            # TODO: some logic that decides when to run which crawler. Copy from JS
            await ((await self._secondary_crawler.get_request_provider()).add_request(deepcopy(context.request)))
            await ((await self._primary_crawler.get_request_provider()).add_request(deepcopy(context.request))) # Each request has to be passed as copy to decouple processing, otherwise even if processed by different request queues it will be still coupled.


            # This will always happen
            primary_crawler_result = await self.coordinator.get_result(self._primary_crawler, context.request.id)

            #This just sometimes
            secondary_crawler_result = await self.coordinator.get_result(self._secondary_crawler, context.request.id)
            # TODO: Do some work with the result, compare, analyze, save them. Enhance context

        # End of "atomic" lock

        """
        content = await s_context.page.content()
        body = await s_context.response.body()
        """
        #context = AdaptiveCrawlingContext.from_playwright_crawling_context(s_context)
        yield primary_crawler_result

    def _connect_crawlers(self):
        """Point storage of subcrawlers to the storage of top crawler or other related connections"""

        # Maybe not needed.
        pass

    async def _keep_crawlers_alive(self) -> None:
        """Make sure that primary and secondary crawler will be alive unless explicitly killed."""
        primary_provider = await self._primary_crawler.get_request_provider()
        secondary_provider = await self._secondary_crawler.get_request_provider()
        primary_provider.keep_alive = True
        secondary_provider.keep_alive = True


    async def _kill_crawlers(self):
        """Stop primary and secondary crawler."""
        primary_provider = await self._primary_crawler.get_request_provider()
        secondary_provider = await self._secondary_crawler.get_request_provider()
        primary_provider.keep_alive = False
        secondary_provider.keep_alive = False


    async def run( self,requests: Sequence[str | Request] | None = None,*,
        purge_request_queue: bool = True) -> FinalStatistics:

        # TODO: Make something robust, maybe some context that keeps track of subcrawlers
        try:
            await self._keep_crawlers_alive()
            tasks = [asyncio.create_task(self._primary_crawler.run()),asyncio.create_task(self._secondary_crawler.run())]

            await super().run(requests=requests, purge_request_queue=purge_request_queue)

        except Exception as e:
            raise e
        finally:
            await self._kill_crawlers()
        await asyncio.gather(*tasks)

        # TODO: Handle stats


# TODO: Make generic based on the crawler result types
class _Coordinator:
    """Some way to share state and results between different crawlers."""
    def __init__(self, crawler1: BasicCrawler, crawler2: BasicCrawler) -> None:
        self._id1=id(crawler1)
        self._id2=id(crawler2)
        self._results = {self._id1: {}, self._id2: {}}
        self.locks = {}


    def register_expected_result(self, request_id: str):
        self._results[self._id1][request_id] = Future()
        self._results[self._id2][request_id] = Future()


    async def get_result(self, crawler: BasicCrawler, request_id: str, timeout: int =1):
        # TODO: Handle timeouts and edge cases
        return await self._results[id(crawler)][request_id]


    def set_result(self, crawler: BasicCrawler, request_id: str, result: Any) -> None:
        self._results[id(crawler)][request_id].set_result(result)


    def remove_result(self, request_id):
        del self._results[self._id1][request_id]
        del self._results[self._id2][request_id]

    def result_cleanup(self, request_id: str):
        return _CoordinatorResultCleanup(self, request_id)



class _CoordinatorResultCleanup:

    def __init__(self, coordinator: _Coordinator, request_id: str):
        self._coordinator = coordinator
        self._request_id = request_id

    async def __aenter__(self):
        self._coordinator.register_expected_result(self._request_id)
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._coordinator.remove_result(self._request_id)
