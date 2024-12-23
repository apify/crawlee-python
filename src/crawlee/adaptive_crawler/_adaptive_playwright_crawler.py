import asyncio
from collections.abc import Coroutine, Callable, Awaitable
from copy import deepcopy
from dataclasses import dataclass, fields
from http.client import responses
from pyexpat import features

from bs4 import BeautifulSoup
from playwright.async_api import Response, Page
from typing_extensions import Unpack

from crawlee import service_locator
from crawlee._types import EnqueueLinksFunction, PushDataFunction, JsonSerializable, PushDataKwargs
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee.adaptive_crawler._adaptive_crawler import AdaptiveCrawler, _Coordinator
from crawlee.basic_crawler import BasicCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.http_clients import HttpResponse
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import RequestQueue
from tests.unit.browsers.test_playwright_browser_controller import playwright

from crawlee._types import HttpHeaders
@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightCrawlingContext(BeautifulSoupCrawlingContext):
    _response: Response | None = None
    _enqueue_links: EnqueueLinksFunction | None = None
    _infinite_scroll: Callable[[], Awaitable[None]] | None = None
    _page : Page | None = None


    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: BeautifulSoupCrawlingContext,
                                            push_data: PushDataFunction,
                                            enqueue_links: EnqueueLinksFunction):
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Override push_data and enqueue_links
        context_kwargs["push_data"] = push_data
        #context_kwargs[enqueue_links] = enqueue_links
        return cls(**context_kwargs)

    @classmethod
    async def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext,
                                            push_data: PushDataFunction,
                                            enqueue_links: EnqueueLinksFunction):

        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        context_kwargs.pop("response")
        context_kwargs.pop("page")
        context_kwargs.pop("infinite_scroll")
        http_response = await _HttpResponse.from_playwright_response(context.response)
        # Override push_data and enqueue_links
        context_kwargs["push_data"] = push_data
        #context_kwargs[enqueue_links] = enqueue_links
        return cls(parsed_content= BeautifulSoup(http_response.read()),
                   _response = context.response,
                   _enqueue_links = enqueue_links,
                   _infinite_scroll = None,
                   http_response = http_response,
                   **context_kwargs)

    @staticmethod
    def create_push_data(coordinator: _Coordinator, crawler: BasicCrawler, request_id: str) ->PushDataFunction:
        async def push_data(data: JsonSerializable,
                      dataset_id: str | None = None,
                      dataset_name: str | None = None,
                      **kwargs: Unpack[PushDataKwargs],
                      ) -> Coroutine[None, None, None]:
            push_data_args = {"data": data, "dataset_id": dataset_id, "dataset_name": dataset_name}
            async def _():
                return coordinator.set_result(crawler, request_id, push_data_kwargs={**push_data_args, **kwargs})
            return await _()
        return push_data

@dataclass(frozen=True)
class _HttpResponse(HttpResponse):

    http_version : str = None
    status_code : int = None
    headers: HttpHeaders = None
    _content: bytes = None

    def read(self):
        return self._content

    @classmethod
    async def from_playwright_response(cls, response: Response):
        headers = HttpHeaders(response.headers)
        status_code = response.status
        http_version = "TODO"
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)

class AdaptivePlayWrightCrawler(AdaptiveCrawler):

    @classmethod
    async def create_with_default_settings(cls, **crawler_kwargs):
        primary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
        secondary_request_provider = await RequestQueue.open(name=crypto_random_object_id())

        # TODO: set subcrawlers log level higher than INFO. So far good for development
        beautifulsoup_crawler = BeautifulSoupCrawler(request_provider=primary_request_provider)
        playwright_crawler = PlaywrightCrawler(request_provider=secondary_request_provider)

        adaptive_crawler = cls(primary_crawler=beautifulsoup_crawler,
                               secondary_crawler=playwright_crawler,
                               **crawler_kwargs)



        @beautifulsoup_crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing with BS: {context.request.url} ...')
            push_data = AdaptivePlaywrightCrawlingContext.create_push_data(
                coordinator=adaptive_crawler.coordinator,
                crawler=beautifulsoup_crawler,
                request_id=context.request.id)

            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_beautifulsoup_crawling_context(
                context=context, push_data=push_data, enqueue_links=context.enqueue_links
            )
            # Send to proper user function through top level crawler router with subcrawler context
            await adaptive_crawler.router(adaptive_crawling_context)


        @playwright_crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing with PW: {context.request.url} ...')
            push_data = AdaptivePlaywrightCrawlingContext.create_push_data(
                coordinator=adaptive_crawler.coordinator,
                crawler=playwright_crawler,
                request_id=context.request.id)
            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                context=context, push_data=push_data, enqueue_links=context.enqueue_links)
            await adaptive_crawler.router(adaptive_crawling_context)

        return adaptive_crawler


