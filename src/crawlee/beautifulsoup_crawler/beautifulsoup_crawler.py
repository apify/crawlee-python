from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Iterable, Literal

import httpx
from typing_extensions import Unpack

from crawlee.basic_crawler.basic_crawler import BasicCrawler
from crawlee.basic_crawler.context_pipeline import ContextPipeline
from crawlee.beautifulsoup_crawler.types import BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.http_clients.httpx_client import HttpxClient
from crawlee.http_crawler.types import HttpCrawlingContext
from crawlee.request import BaseRequestData

if TYPE_CHECKING:
    from datetime import timedelta

    from crawlee.autoscaling.autoscaled_pool import ConcurrencySettings
    from crawlee.basic_crawler.types import AddRequestsFunctionKwargs, BasicCrawlingContext
    from crawlee.configuration import Configuration
    from crawlee.storages.request_provider import RequestProvider


class BeautifulSoupCrawler(BasicCrawler[BeautifulSoupCrawlingContext]):
    """A crawler that fetches the request URL using `httpx` and parses the result with `BeautifulSoup`."""

    def __init__(
        self,
        *,
        parser: Literal['html.parser', 'lxml', 'xml', 'html5lib'] = 'lxml',
        request_provider: RequestProvider | None = None,
        router: Callable[[BeautifulSoupCrawlingContext], Awaitable[None]] | None = None,
        concurrency_settings: ConcurrencySettings | None = None,
        configuration: Configuration | None = None,
        request_handler_timeout: timedelta | None = None,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
    ) -> None:
        """Initialize the BeautifulSoupCrawler.

        Args:
            parser: The type of parser that should be used by BeautifulSoup

            request_provider: Provides requests to be processed

            router: A callable to which request handling is delegated

            concurrency_settings: Allows fine-tuning concurrency levels

            configuration: Crawler configuration

            request_handler_timeout: How long is a single request handler allowed to run

            additional_http_error_status_codes: HTTP status codes that should be considered errors (and trigger a retry)

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but we want to treat
                them as successful
        """
        context_pipeline = ContextPipeline().compose(self._make_http_request).compose(self._parse_http_response)

        self._client = httpx.AsyncClient()
        self._parser = parser

        basic_crawler_kwargs = {}

        if request_handler_timeout is not None:
            basic_crawler_kwargs['request_handler_timeout'] = request_handler_timeout

        super().__init__(
            request_provider=request_provider,
            router=router,
            concurrency_settings=concurrency_settings,
            configuration=configuration,
            http_client=HttpxClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
            _context_pipeline=context_pipeline,
            **basic_crawler_kwargs,  # type: ignore
        )

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await self._http_client.crawl(context.request)

        yield HttpCrawlingContext(
            request=context.request,
            send_request=context.send_request,
            add_requests=context.add_requests,
            http_response=result.http_response,
        )

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        from bs4 import BeautifulSoup, Tag

        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))

        async def enqueue_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict[str, Any] | None = None,
            **kwargs: Unpack[AddRequestsFunctionKwargs],
        ) -> None:
            requests = list[BaseRequestData]()
            user_data = user_data or {}

            link: Tag
            for link in soup.find_all(selector):
                link_user_data = user_data

                if label is not None:
                    link_user_data.setdefault('label', label)

                if (href := link.attrs.get('href')) is not None:
                    requests.append(BaseRequestData.from_url(href, user_data=link_user_data))

            uses_patterns = 'include' in kwargs or 'exclude' in kwargs
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME if uses_patterns else EnqueueStrategy.ALL)

            await context.add_requests(requests, **kwargs)

        yield BeautifulSoupCrawlingContext(
            request=context.request,
            send_request=context.send_request,
            add_requests=context.add_requests,
            enqueue_links=enqueue_links,
            http_response=context.http_response,
            soup=soup,
        )
