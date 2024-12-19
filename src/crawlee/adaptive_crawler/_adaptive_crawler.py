import asyncio
from collections.abc import AsyncGenerator, Sequence
from itertools import cycle
from logging import getLogger

from typing_extensions import Unpack

from crawlee import Request
from crawlee._types import BasicCrawlingContext
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.statistics import FinalStatistics
from crawlee.storages import RequestQueue
getLogger("crawlee._autoscaling.autoscaled_pool").setLevel("DEBUG")
getLogger("crawlee.storages._request_queue").setLevel("DEBUG")

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
        kwargs['_context_pipeline'] = ContextPipeline().compose(self._delegate_to_crawlers)
        # Some way of toggling. TODO: Copy from JS in the future
        self.use_secondary_crawler = cycle((False, True))
        super().__init__(**kwargs)


    async def _delegate_to_crawlers(self, context: BasicCrawlingContext) -> AsyncGenerator[BasicCrawlingContext, None]:
        #if next(self.use_secondary_crawler):
        await ((await self._primary_crawler.get_request_provider()).add_request(context.request))
        await ((await self._secondary_crawler.get_request_provider()).add_request(context.request))

        yield context

    def _connect_crawlers(self):
        """Point storage of subcrawlers to the storage of top crawler"""
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

        await self._keep_crawlers_alive()
        tasks = [asyncio.create_task(self._primary_crawler.run()),asyncio.create_task(self._secondary_crawler.run())]
        await super().run(requests=requests, purge_request_queue=purge_request_queue)
        await self._kill_crawlers()
        await asyncio.gather(*tasks)
