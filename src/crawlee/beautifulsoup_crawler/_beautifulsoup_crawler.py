from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, AsyncGenerator, Iterable, Literal, Optional

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError
from typing_extensions import Unpack

from crawlee import EnqueueStrategy
from crawlee._request import BaseRequestData
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.beautifulsoup_crawler._beautifulsoup_crawling_context import BeautifulSoupCrawlingContext
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient
from crawlee.http_crawler import HttpCrawlingContext

if TYPE_CHECKING:
    from crawlee._types import AddRequestsKwargs, BasicCrawlingContext


class BeautifulSoupCrawler(BasicCrawler[BeautifulSoupCrawlingContext]):
    """Crawler that fetches a URL using `httpx` and parses the result with `BeautifulSoup`."""

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
            parser: The parser to use with BeautifulSoup (e.g., 'lxml').
            additional_http_error_status_codes: HTTP status codes treated as errors.
            ignore_http_error_status_codes: Status codes to be treated as successful responses.
            kwargs: Arguments forwarded to BasicCrawler.
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
            HttpxHttpClient(
                additional_http_error_status_codes=additional_http_error_status_codes,
                ignore_http_error_status_codes=ignore_http_error_status_codes,
            ),
        )

        kwargs.setdefault('_logger', logging.getLogger(__name__))

        super().__init__(**kwargs)

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        """Perform the HTTP request and yield the result."""
        result = await self._http_client.crawl(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            statistics=self._statistics,
        )

        yield HttpCrawlingContext(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            get_key_value_store=context.get_key_value_store,
            log=context.log,
            http_response=result.http_response,
        )

    async def _handle_blocked_request(
        self, crawling_context: BeautifulSoupCrawlingContext
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        """Handle cases where the request is blocked."""
        if self._retry_on_blocked:
            status_code = crawling_context.http_response.status_code

            if crawling_context.session and crawling_context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Session blocked by status code {status_code}')

            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if crawling_context.soup.select_one(selector) is not None
            ]

            if matched_selectors:
                raise SessionError(
                    'Request blocked - '
                    f'Matched selectors: {", ".join(matched_selectors)}'
                )

        yield crawling_context

    async def _parse_http_response(
        self,
        context: HttpCrawlingContext,
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        """Parse the HTTP response with BeautifulSoup and yield the result."""
        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))

        async def enqueue_links(
            *,
            selector: str = 'a',
            label: Optional[str] = None,
            user_data: Optional[dict[str, Any]] = None,
            **kwargs: Unpack[AddRequestsKwargs],
        ) -> None:
            """Enqueue links found in the HTML."""
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

            requests = []
            user_data = user_data or {}

            for link in soup.select(selector):
                link_user_data = user_data.copy()

                if label:
                    link_user_data.setdefault('label', label)

                if (url := link.attrs.get('href')) is not None:
                    url = url.strip()

                    if not is_url_absolute(url):
                        url = convert_to_absolute_url(context.request.url, url)

                    try:
                        request = BaseRequestData.from_url(url, user_data=link_user_data)
                    except ValidationError as exc:
                        context.log.debug(f'Skipping URL "{url}" due to validation error: {exc}')
                        continue

                    requests.append(request)

            await context.add_requests(requests, **kwargs)

        yield BeautifulSoupCrawlingContext(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            enqueue_links=enqueue_links,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            get_key_value_store=context.get_key_value_store,
            log=context.log,
            http_response=context.http_response,
            soup=soup,
        )
