import asyncio
from collections.abc import Coroutine, Callable, Awaitable, Sequence
from copy import deepcopy
from dataclasses import dataclass, fields
from http.client import responses
from pyexpat import features

from bs4 import BeautifulSoup
from playwright.async_api import Response, Page
from typing_extensions import Unpack

from crawlee import service_locator
from crawlee._request import BaseRequestData, Request
from crawlee._types import EnqueueLinksFunction, PushDataFunction, JsonSerializable, PushDataKwargs, \
    AddRequestsFunction, EnqueueLinksKwargs
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee.adaptive_crawler._adaptive_crawler import AdaptiveCrawler, _Coordinator
from crawlee.basic_crawler import BasicCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.beautifulsoup_crawler._beautifulsoup_parser import BeautifulSoupParser, BeautifulSoupParserType
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
    _infinite_scroll: Callable[[], Awaitable[None]] | None = None
    _page : Page | None = None
    # TODO: UseStateFunction


    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: BeautifulSoupCrawlingContext,
                                            push_data: PushDataFunction,
                                            add_requests: AddRequestsFunction):
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Override push_data and enqueue_links
        context_kwargs["push_data"] = push_data
        context_kwargs["add_requests"] = add_requests
        return cls(**context_kwargs)

    @classmethod
    async def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext,
                                            push_data: PushDataFunction,
                                            add_requests: AddRequestsFunction):

        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Remove playwright specific
        context_kwargs.pop("response")
        context_kwargs.pop("page")
        context_kwargs.pop("infinite_scroll")
        http_response = await _HttpResponse.from_playwright_response(context.response)
        # Override push_data and enqueue_links
        context_kwargs["push_data"] = push_data
        context_kwargs["add_requests"] = add_requests
        #context_kwargs[enqueue_links] = enqueue_links
        return cls(parsed_content= BeautifulSoup(http_response.read(), features="lxml"), # TODO: Pass parser type
                   _response = context.response,
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
                return coordinator.set_push_data(crawler, request_id, push_data_kwargs={**push_data_args, **kwargs})
            return await _()
        return push_data

    @staticmethod
    def create_add_requests(coordinator: _Coordinator, crawler: BasicCrawler, request_id: str) ->AddRequestsFunction:
        async def add_request(requests: Sequence[str | BaseRequestData | Request],
                              **kwargs: Unpack[EnqueueLinksKwargs]) -> Coroutine[None, None, None]:
            async def _():
                return coordinator.add_request(crawler, request_id, add_request_kwargs={"requests": requests, **kwargs})
            return await _()
        return add_request

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

    async def reroute_to_top_handler(self, sub_crawler: BasicCrawler, context: AdaptivePlaywrightCrawlingContext, request_id: str):
        try:
            await self.router(context)
        except Exception as e:
            # Finalize results only after all retries have been done.
            if (context.request.retry_count + 1) >= (context.request.max_retries or self._max_request_retries):
                self.coordinator.finalize_result(crawler=sub_crawler,
                                                             request_id=context.request.id)
            raise e
        # Finalize results if no error.
        self.coordinator.finalize_result(crawler=sub_crawler,
                                                     request_id=context.request.id)

    @classmethod
    async def create_with_default_settings(cls, **crawler_kwargs):
        beautifulsoup_crawler_kwargs = {**crawler_kwargs, **{"parser": "lxml"}}
        playwright_crawler_kwargs = {**crawler_kwargs}
        return await cls.create_with_custom_settings(crawler_kwargs = crawler_kwargs,
                                        beautiful_crawler_soup_kwargs = beautifulsoup_crawler_kwargs,
                                        playwright_crawler_kwargs = playwright_crawler_kwargs)

    @classmethod
    async def create_with_custom_settings(cls, crawler_kwargs: dict | None,
                                           beautiful_crawler_soup_kwargs: dict | None,
                                           playwright_crawler_kwargs: dict | None):
        crawler_kwargs = crawler_kwargs or {}
        beautiful_crawler_soup_kwargs = beautiful_crawler_soup_kwargs or {}
        playwright_crawler_kwargs = playwright_crawler_kwargs or {}

        primary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
        secondary_request_provider = await RequestQueue.open(name=crypto_random_object_id())

        # TODO: set subcrawlers log level higher than INFO. So far good for development
        beautifulsoup_crawler = BeautifulSoupCrawler(request_provider=primary_request_provider, **{**crawler_kwargs, **beautiful_crawler_soup_kwargs})
        playwright_crawler = PlaywrightCrawler(request_provider=secondary_request_provider, **{**crawler_kwargs, **playwright_crawler_kwargs})

        adaptive_crawler = await cls.from_crawlers(primary_crawler=beautifulsoup_crawler,
                               secondary_crawler=playwright_crawler,
                               **crawler_kwargs)


        @beautifulsoup_crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing with BS: {context.request.url} ...')

            push_data = AdaptivePlaywrightCrawlingContext.create_push_data(
                coordinator=adaptive_crawler.coordinator,
                crawler=beautifulsoup_crawler,
                request_id=context.request.id)

            add_requests = AdaptivePlaywrightCrawlingContext.create_add_requests(
                coordinator=adaptive_crawler.coordinator,
                crawler=beautifulsoup_crawler,
                request_id=context.request.id)

            adaptive_crawling_context = AdaptivePlaywrightCrawlingContext.from_beautifulsoup_crawling_context(
                context=context, push_data=push_data, add_requests=add_requests
            )

            await adaptive_crawler.reroute_to_top_handler(sub_crawler=playwright_crawler,
                                                          context=adaptive_crawling_context,
                                                          request_id=context.request.id)


        @playwright_crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing with PW: {context.request.url} ...')

            push_data = AdaptivePlaywrightCrawlingContext.create_push_data(
                coordinator=adaptive_crawler.coordinator,
                crawler=playwright_crawler,
                request_id=context.request.id)

            add_requests = AdaptivePlaywrightCrawlingContext.create_add_requests(
                coordinator=adaptive_crawler.coordinator,
                crawler=playwright_crawler,
                request_id=context.request.id)

            adaptive_crawling_context = await AdaptivePlaywrightCrawlingContext.from_playwright_crawling_context(
                context=context, push_data=push_data, add_requests=add_requests)

            await adaptive_crawler.reroute_to_top_handler(sub_crawler=beautifulsoup_crawler,
                                                          context=adaptive_crawling_context,
                                                          request_id=context.request.id)

        return adaptive_crawler


