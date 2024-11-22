from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, AsyncGenerator, Iterable
from warnings import warn

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError
from typing_extensions import Self, Generic,TypeVar, Unpack, override, reveal_type

from crawlee._request import BaseRequestData
from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, EnqueueLinksKwargs, EnqueueStrategy
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpResponse, HttpxHttpClient
from crawlee.http_crawler import HttpCrawlingContext

TParseResult = TypeVar('TParseResult', default=bytes)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
@docs_group('Data structures')
class ParsedHttpCrawlingContext(Generic[TParseResult], HttpCrawlingContext):
    """The crawling context used by _HttpCrawler.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    parsed_content: TParseResult
    enqueue_links: EnqueueLinksFunction

    @classmethod
    def from_http_crawling_context(
        cls, context: HttpCrawlingContext, parsed_content: TParseResult, enqueue_links: EnqueueLinksFunction
    ) -> Self:
        """Convenience constructor that creates new context from existing HttpCrawlingContext."""
        context_kwargs = {field.name: getattr(context, field.name) for field in fields(context)}
        return cls(parsed_content=parsed_content, enqueue_links=enqueue_links, **context_kwargs)

    @property
    def soup(self) -> BeautifulSoup:
        """Property for backwards compatibility."""
        if isinstance(self.parsed_content, BeautifulSoup):
            warn('Usage of deprecated property soup. Use parsed_content instead.', DeprecationWarning, stacklevel=2)
            return self.parsed_content
        raise RuntimeError(
            'Trying to access soup property on context that does not have BeautifulSoup in parsed_content.'
            'Access parsed_content instead.'
        )


@dataclass(frozen=True)
class BlockedInfo:
    """Information about whether the crawling is blocked. If reason is empty, then it means it is not blocked."""

    reason: str

    def __bool__(self) -> bool:
        """No reason means no blocking."""
        return bool(self.reason)


class StaticContentParser(Generic[TParseResult], ABC):
    """Parser used for parsing http response and inspecting parsed result to find links or detect blocking."""

    @abstractmethod
    def parse(self, http_response: HttpResponse) -> TParseResult:
        """Parse http response."""
        ...

    @abstractmethod
    def is_blocked(self, result: TParseResult) -> BlockedInfo:
        """Detect if blocked and return BlockedInfo with additional information."""
        ...

    @abstractmethod
    def find_links(self, result: TParseResult, selector: str) -> Iterable[str]:
        """Find all links in result using selector."""
        ...


class NoParser(StaticContentParser[bytes]):
    """Dummy parser mainly for backwards compatibility.

    To enable using HttpCrawler without need for additional specific parser.
    """

    @override
    def parse(self, http_response: HttpResponse) -> bytes:
        return http_response.read()

    @override
    def is_blocked(self, result: bytes) -> BlockedInfo:  # Intentional unused argument.
        return BlockedInfo(reason='')

    @override
    def find_links(self, result: bytes, selector: str) -> Iterable[str]:  # Intentional unused argument.
        return []


class BeautifulSoupContentParser(StaticContentParser[BeautifulSoup]):
    """Parser for parsing http response using BeautifulSoup."""

    def __init__(self, parser: str = 'lxml') -> None:
        self._parser = parser

    @override
    def parse(self, response: HttpResponse) -> BeautifulSoup:
        return BeautifulSoup(response.read(), parser=self._parser)

    @override
    def is_blocked(self, result: BeautifulSoup) -> BlockedInfo:
        reason = ''
        if result.soup is not None:
            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if result.soup.select_one(selector) is not None
            ]
            if matched_selectors:
                reason = f"Assuming the session is blocked - HTTP response matched the following selectors: {'; '.join(
                    matched_selectors)}"
        return BlockedInfo(reason=reason)

    @override
    def find_links(self, soup: BeautifulSoup, selector: str) -> Iterable[str]:
        link: Tag
        urls: list[str] = []
        for link in soup.select(selector):
            if (url := link.attrs.get('href')) is not None:
                urls.append(url.strip())  # noqa: PERF401  #Mypy has problems using is not None for type inference in list comprehension.
        return urls


class _HttpCrawler(Generic[TParseResult], BasicCrawler[HttpCrawlingContext]):
    """A web crawler for performing HTTP requests.

    The `_HttpCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features. On top
    of that it implements the HTTP communication using the HTTP clients. The class allows integration with
    any HTTP client that implements the `BaseHttpClient` interface. The HTTP client is provided to the crawler
    as an input parameter to the constructor.

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using a browser-based crawler like the `PlaywrightCrawler`.

    ### Usage

    ```python
    from crawlee.http_crawler import _HttpCrawler, HttpCrawlingContext

    crawler = _HttpCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'response': context.http_response.read().decode()[:100],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    await crawler.run(['https://crawlee.dev/'])
    ```
    """

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

    async def _parse_http_response(
        self, context: HttpCrawlingContext
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        parsed_content = self.parser.parse(context.http_response)
        yield ParsedHttpCrawlingContext.from_http_crawling_context(
            context=context,
            parsed_content=parsed_content,
            enqueue_links=self._create_enqueue_links_callback(context, parsed_content),
        )

    def _create_enqueue_links_callback(
        self, context: HttpCrawlingContext, parsed_content: TParseResult
    ) -> EnqueueLinksFunction:
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
            for link in self.parser.find_links(parsed_content, selector=selector):
                url = link
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

        yield HttpCrawlingContext.from_basic_crawling_context(context=context, http_response=result.http_response)

    async def _handle_blocked_request(self, context: ParsedHttpCrawlingContext[TParseResult]) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
        """Try to detect if the request is blocked based on the HTTP status code or the parsed response content.

        Args:
            context: The current crawling context.

        Raises:
            SessionError: If the request is considered blocked.

        Yields:
            The original crawling context if no errors are detected.
        """
        if self._retry_on_blocked:
            status_code = context.http_response.status_code

            # TODO: refactor to avoid private member access
            # https://github.com/apify/crawlee-python/issues/708
            if (
                context.session
                and status_code not in self._http_client._ignore_http_error_status_codes  # noqa: SLF001
                and context.session.is_blocked_status_code(status_code=status_code)
            ):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}')
            if blocked_info := self.parser.is_blocked(context.parsed_content):
                raise SessionError(blocked_info.reason)
        yield context

class HttpCrawler(_HttpCrawler[bytes]):
    def __init__(
        self,
        *,
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[HttpCrawlingContext]],
    ) -> None:
        """
        I didn't find another way how to make default constructor specifying one of type on generics.
        """
        super().__init__(
            parser=NoParser(),
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
            **kwargs,
        )

