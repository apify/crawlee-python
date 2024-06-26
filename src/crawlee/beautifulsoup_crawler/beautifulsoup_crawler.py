from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, AsyncGenerator, Iterable, Literal

from bs4 import BeautifulSoup, Tag
from typing_extensions import Unpack

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.basic_crawler.errors import SessionError
from crawlee.beautifulsoup_crawler.types import BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.http_clients import HttpxClient
from crawlee.http_crawler import HttpCrawlingContext
from crawlee.models import BaseRequestData

if TYPE_CHECKING:
    from crawlee.basic_crawler.types import AddRequestsKwargs, BasicCrawlingContext


class BeautifulSoupCrawler(BasicCrawler[BeautifulSoupCrawlingContext]):
    """A crawler that fetches the request URL using `httpx` and parses the result with `BeautifulSoup`."""

    def __init__(
        self,
        *,
        parser: Literal['html.parser', 'lxml', 'xml', 'html5lib'] = 'lxml',
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[BeautifulSoupCrawlingContext]],
    ) -> None:
        """Initialize the BeautifulSoupCrawler.

        Args:
            parser: The type of parser that should be used by BeautifulSoup

            additional_http_error_status_codes: HTTP status codes that should be considered errors (and trigger a retry)

            ignore_http_error_status_codes: HTTP status codes that are normally considered errors but we want to treat
                them as successful

            kwargs: Arguments to be forwarded to the underlying BasicCrawler
        """
        self._parser = parser

        kwargs['_context_pipeline'] = (
            ContextPipeline()
            .compose(self._make_http_request)
            .compose(self._parse_http_response)
            .compose(self._handle_blocked_request)
        )

        kwargs.setdefault(
            'http_client',
            HttpxClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
        )

        kwargs.setdefault('_logger', logging.getLogger(__name__))

        super().__init__(**kwargs)

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await self._http_client.crawl(
            context.request,
            context.session,
            context.proxy_info,
            self._statistics,
        )

        yield HttpCrawlingContext(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            log=context.log,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(
        self, crawling_context: BeautifulSoupCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        if self._retry_on_blocked:
            status_code = crawling_context.http_response.status_code

            if crawling_context.session and crawling_context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')

            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if crawling_context.soup.select_one(selector) is not None
            ]

            if matched_selectors:
                raise SessionError(
                    'Assuming the session is blocked - '
                    f"HTTP response matched the following selectors: {'; '.join(matched_selectors)}"
                )

        yield crawling_context

    async def _parse_http_response(
        self,
        context: HttpCrawlingContext,
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))

        async def enqueue_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict[str, Any] | None = None,
            **kwargs: Unpack[AddRequestsKwargs],
        ) -> None:
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

            requests = list[BaseRequestData]()
            user_data = user_data or {}

            link: Tag
            for link in soup.select(selector):
                link_user_data = user_data

                if label is not None:
                    link_user_data.setdefault('label', label)

                if (href := link.attrs.get('href')) is not None:
                    requests.append(BaseRequestData.from_url(href, user_data=link_user_data))

            await context.add_requests(requests, **kwargs)

        yield BeautifulSoupCrawlingContext(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            enqueue_links=enqueue_links,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            log=context.log,
            http_response=context.http_response,
            soup=soup,
        )
