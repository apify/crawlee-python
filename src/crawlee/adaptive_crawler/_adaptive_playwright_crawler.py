from __future__ import annotations


from collections.abc import Coroutine, Callable, Awaitable, Sequence

from dataclasses import dataclass, fields

from typing import Self

from bs4 import BeautifulSoup
from playwright.async_api import Response, Page
from typing_extensions import Unpack


from crawlee._request import BaseRequestData, Request
from crawlee._types import PushDataFunction, JsonSerializable, PushDataKwargs, \
    AddRequestsFunction, EnqueueLinksKwargs
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group
from crawlee.adaptive_crawler._adaptive_crawler import AdaptiveCrawler, _Coordinator
from crawlee.basic_crawler import BasicCrawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext, BeautifulSoupParserType

from crawlee.http_clients import HttpResponse

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import RequestQueue


from crawlee._types import HttpHeaders

@dataclass(frozen=True)
@docs_group('Data structures')
class AdaptivePlaywrightCrawlingContext(BeautifulSoupCrawlingContext):
    _response: Response | None = None
    _infinite_scroll: Callable[[], Awaitable[None]] | None = None
    _page : Page | None = None
    # TODO: UseStateFunction

    @property
    def page(self):
        return self._page

    @property
    def infinite_scroll(self):
        return self._infinite_scroll

    @property
    def response(self):
        return self._response

    @classmethod
    def from_beautifulsoup_crawling_context(cls, context: BeautifulSoupCrawlingContext,
                                            push_data: PushDataFunction,
                                            add_requests: AddRequestsFunction):
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Override push_data and add_request
        context_kwargs["push_data"] = push_data
        context_kwargs["add_requests"] = add_requests
        return cls(**context_kwargs)

    @classmethod
    async def from_playwright_crawling_context(cls, context: PlaywrightCrawlingContext,
                                            push_data: PushDataFunction,
                                            add_requests: AddRequestsFunction,
                                            beautiful_soup_parser_type: BeautifulSoupParserType):

        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        # Remove playwright specific attributes and pass them as private instead.
        context_kwargs["_response"] = context_kwargs.pop("response")
        context_kwargs["_page"] = context_kwargs.pop("page")
        context_kwargs["_infinite_scroll"] = context_kwargs.pop("infinite_scroll")
        http_response = await _HttpResponse.from_playwright_response(context.response)
        # Override push_data and enqueue_links
        context_kwargs["push_data"] = push_data
        context_kwargs["add_requests"] = add_requests
        return cls(parsed_content= BeautifulSoup(http_response.read(), features=beautiful_soup_parser_type),
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
            return coordinator.set_push_data(crawler, request_id, push_data_kwargs={**push_data_args, **kwargs})
        return push_data

    @staticmethod
    def create_add_requests(coordinator: _Coordinator, crawler: BasicCrawler, request_id: str) ->AddRequestsFunction:
        async def add_request(requests: Sequence[str | BaseRequestData | Request],
                              **kwargs: Unpack[EnqueueLinksKwargs]) -> Coroutine[None, None, None]:
            return coordinator.set_add_request(crawler, request_id, add_request_kwargs={"requests": requests, **kwargs})
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
        # Can't find this anywhere in PlayWright, but some headers can include information about protocol. In firefox for example: 'x-firefox-spdy'
        # Might be also obtained by executing JS code in browser: performance.getEntries()[0].nextHopProtocol
        # Response headers capitalization not respecting http1.1 Pascal case. Always lower case in PlayWright.
        http_version = "TODO"
        _content = await response.body()

        return cls(http_version=http_version, status_code=status_code, headers=headers, _content=_content)

class AdaptivePlaywrightCrawler(AdaptiveCrawler[AdaptivePlaywrightCrawlingContext]):

    @classmethod
    async def create_with_default_settings(cls, **crawler_kwargs):
        beautifulsoup_crawler_kwargs = {**crawler_kwargs, "parser": "lxml"}
        playwright_crawler_kwargs = {**crawler_kwargs, "headless": True}

        return await cls.create_with_custom_settings(crawler_kwargs = crawler_kwargs,
                                        beautifulsoup_crawler_kwargs = beautifulsoup_crawler_kwargs,
                                        playwright_crawler_kwargs = playwright_crawler_kwargs)

    @classmethod
    async def create_with_custom_settings(cls, crawler_kwargs: dict | None,
                                           beautifulsoup_crawler_kwargs: dict | None,
                                           playwright_crawler_kwargs: dict | None):
        crawler_kwargs = crawler_kwargs or {}
        beautifulsoup_crawler_kwargs = beautifulsoup_crawler_kwargs or {}
        playwright_crawler_kwargs = playwright_crawler_kwargs or {}
        primary_request_provider = await RequestQueue.open(name=crypto_random_object_id())
        secondary_request_provider = await RequestQueue.open(name=crypto_random_object_id())

        # TODO: set subcrawlers log level higher than INFO. So far good for development
        beautifulsoup_crawler = BeautifulSoupCrawler(request_provider=primary_request_provider, **{**crawler_kwargs, **beautifulsoup_crawler_kwargs})
        playwright_crawler = PlaywrightCrawler(request_provider=secondary_request_provider, **{**crawler_kwargs, **playwright_crawler_kwargs})

        adaptive_crawler = await cls.from_crawlers(primary_crawler=beautifulsoup_crawler,
                               secondary_crawler=playwright_crawler,
                               **crawler_kwargs)


        @beautifulsoup_crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            """Route sub crawler router to top crawler router instead.
            Call top crawler router on adaptive context and route all results to coordinator."""
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

            await adaptive_crawler.reroute_to_top_handler(sub_crawler=beautifulsoup_crawler,
                                                          context=adaptive_crawling_context,
                                                          request_id=context.request.id)


        @playwright_crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            """Route sub crawler router to top crawler router instead.
            Call top crawler router on adaptive context and route all results to coordinator."""
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
                context=context, push_data=push_data, add_requests=add_requests, beautiful_soup_parser_type=beautifulsoup_crawler_kwargs.get("parser"))

            await adaptive_crawler.reroute_to_top_handler(sub_crawler=playwright_crawler,
                                                          context=adaptive_crawling_context,
                                                          request_id=context.request.id)

        return adaptive_crawler


    async def reroute_to_top_handler(self, sub_crawler: BasicCrawler, context: AdaptivePlaywrightCrawlingContext, request_id: str):
        """Call top crawler router and finalize sub crawler results when completed."""
        try:
            await self.router(context)
        except Exception as e:
            # Finalize results only after all retries have been done.
            if (context.request.retry_count + 1) >= (context.request.max_retries or self._max_request_retries):
                self.coordinator.set_exception(crawler=sub_crawler,request_id=context.request.id, exception=e)
                self.coordinator.finalize_result(crawler=sub_crawler,request_id=context.request.id)
                return
            raise e
        # Finalize results if no error.
        self.coordinator.finalize_result(crawler=sub_crawler,
                                                     request_id=context.request.id)
