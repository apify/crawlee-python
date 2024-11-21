from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, AsyncGenerator, Generic, Iterable, Self, TypeVar, Unpack, override

from bs4 import BeautifulSoup
from pydantic import ValidationError

from crawlee._request import BaseRequestData
from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, EnqueueLinksKwargs, EnqueueStrategy
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpCrawlingResult, HttpResponse, HttpxHttpClient
from crawlee.http_crawler import HttpCrawlingContext

TParseResult = TypeVar('TParseResult')
TCrawlingResult = TypeVar('TParseResult')
@dataclass
class BeautifulSoupResult:
    soup: BeautifulSoup

TypeNoParserResult = None

@dataclass(frozen=True)
@docs_group('Data structures')
class ParsedHttpCrawlingContext(Generic[TParseResult], HttpCrawlingContext):
    """Replaces BeautifulSoupCrawlingContext and ParselCrawlingContext"""
    parsed_content: TParseResult
    enqueue_links: EnqueueLinksFunction

    @classmethod
    def fromHttpCrawlingContext(cls, context: HttpCrawlingContext, parsed_content: TParseResult, enqueue_links:EnqueueLinksFunction) -> Self:
        return cls(parsed_content=parsed_content, enqueue_links= enqueue_links, **asdict(context))


class StaticContentParser(Generic[TParseResult], ABC):

    @abstractmethod
    def parse(self, http_response: HttpResponse)->TParseResult:...

    @abstractmethod
    def raise_if_blocked(self, result: TParseResult) -> None: ...

    @abstractmethod
    def find_links(self, result: TParseResult, selector: str) -> Iterable[str]: ...


class BeautifulSoupContentParser(StaticContentParser[BeautifulSoupResult]):
    def __init__(self, parser: str = 'lxml'):
        self.parser = parser

    @override
    def parse(self, response: HttpCrawlingResult)->BeautifulSoupResult:
        return BeautifulSoup(response.read(), self._parser)

    @override
    def raise_if_blocked(self, result: TParseResult) -> None:
        matched_selectors = [
            selector for selector in RETRY_CSS_SELECTORS if result.soup.select_one(selector) is not None
        ]
        if matched_selectors:
            raise SessionError(
                'Assuming the session is blocked - '
                f"HTTP response matched the following selectors: {'; '.join(matched_selectors)}"
            )
        #return matched_selectors  # ??? Really needed ???

    @override
    def find_links(self, result: BeautifulSoupResult, selector: str) -> Iterable[str]:
        urls: str = []
        for link in result.soup.select(selector):
            if (url := link.attrs.get('href')) is not None:
                urls.append(url.strip())
        return urls

class HttpCrawler(Generic[TParseResult], BasicCrawler[HttpCrawlingContext]):
    def __init__(
        self,
        *,
        parser: StaticContentParser[TParseResult],
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[HttpCrawlingContext]],
    ) -> None:
        self.parser = parser

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

    def _parse_http_response(self, context: HttpCrawlingContext) ->ParsedHttpCrawlingContext[TParseResult]:
        return ParsedHttpCrawlingContext.fromHttpCrawlingContext(
            parsed_content=self.parser.parse(context.http_response),
            enqueue_links=self._create_enqueue_links_callback(context)
        )

    def _create_enqueue_links_callback(self, context: HttpCrawlingContext) -> EnqueueLinksFunction:
        async def enqueue_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict[str, Any] | None = None,
            **kwargs: Unpack[EnqueueLinksKwargs],
        ) -> None:
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

            requests = list[BaseRequestData]()
            user_data = user_data or {}
            if label is not None:
                user_data.setdefault('label', label)
            for url in self.parser.find_links(selector):
                if not is_url_absolute(url):
                    url = convert_to_absolute_url(context.request.url, url)
                try:
                    request = BaseRequestData.from_url(url, user_data=user_data)
                except ValidationError as exc:
                    context.log.debug(
                        f'Skipping URL "{url}" due to invalid format: {exc}. '
                        'This may be caused by a malformed URL or unsupported URL scheme. '
                        'Please ensure the URL is correct and retry.'
                    )
                    continue

                requests.append(request)

            await context.add_requests(requests, **kwargs)
        return enqueue_links

    async def _make_http_request(self, context: BasicCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        result = await self._http_client.crawl(
            request=context.request,
            session=context.session,
            proxy_info=context.proxy_info,
            statistics=self._statistics,
        )

        yield HttpCrawlingContext.fromBasicCrawlingContext(context=context, http_response=result.http_response)

    async def _handle_blocked_request(self, context: HttpCrawlingContext) -> AsyncGenerator[HttpCrawlingContext, None]:
        if self._retry_on_blocked:
            status_code = context.http_response.status_code
            if (
                context.session
                and status_code not in self._http_client._ignore_http_error_status_codes  # noqa: SLF001
                and context.session.is_blocked_status_code(status_code=status_code)
            ):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')
            self.parser.raise_if_blocked()
        yield context



class BeautifulSoupCrawler(HttpCrawler[BeautifulSoupResult]):
    ...


beautiful_soup_crawler = BeautifulSoupCrawler(parser=BeautifulSoupContentParser())


