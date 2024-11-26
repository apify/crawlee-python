from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Generic, Iterable

from pydantic import ValidationError

from crawlee import EnqueueStrategy
from crawlee._request import BaseRequestData
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.http_clients import HttpxHttpClient

from ._http_crawling_context import HttpCrawlingContext, ParsedHttpCrawlingContext, TParseResult
from ._http_parser import NoParser, StaticContentParser

if TYPE_CHECKING:
    from typing_extensions import Any, AsyncGenerator, Unpack

    from crawlee._types import BasicCrawlingContext, EnqueueLinksFunction, EnqueueLinksKwargs

logger = logging.getLogger(__name__)


class HttpCrawlerGeneric(Generic[TParseResult], BasicCrawler[ParsedHttpCrawlingContext[TParseResult]]):
    """A web crawler for performing HTTP requests.

    The `HttpCrawlerGeneric` builds on top of the `BasicCrawler`, which means it inherits all of its features. On top
    of that it implements the HTTP communication using the HTTP clients. The class allows integration with
    any HTTP client that implements the `BaseHttpClient` interface. The HTTP client is provided to the crawler
    as an input parameter to the constructor.
    HttpCrawlerGeneric is generic class and is expected to be used together with specific parser that will be used to
    parse http response. See prepared specific version of it: BeautifulSoupCrawler or ParselCrawler for example.
    (For backwards compatibility you can use already specific version HttpCrawler, which uses dummy
    parser.)

    The HTTP client-based crawlers are ideal for websites that do not require JavaScript execution. However,
    if you need to execute client-side JavaScript, consider using a browser-based crawler like the `PlaywrightCrawler`.
    """

    def __init__(
        self,
        *,
        parser: StaticContentParser[TParseResult],
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[TParseResult]]],
    ) -> None:
        self.parser = parser

        kwargs['_context_pipeline'] = (
            ContextPipeline[ParsedHttpCrawlingContext[TParseResult]]()
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
        parsed_content = await self.parser.parse(context.http_response)
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

    async def _handle_blocked_request(
        self, context: ParsedHttpCrawlingContext[TParseResult]
    ) -> AsyncGenerator[ParsedHttpCrawlingContext[TParseResult], None]:
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


class HttpCrawler(HttpCrawlerGeneric[bytes]):
    """Specific version of generic HttpCrawlerGeneric.

    It uses dummy parser NoParser. It is not intended to be used in new code, it is backwards compatibility class.
    In new code either use HttpCrawlerGeneric or other specific children of it - like BeautifulSoupCrawler or
    ParselCrawler.

    ### Usage

    ```python
    from crawlee.http_crawler import HttpCrawlerGeneric, HttpCrawlingContext

    crawler = HttpCrawler()

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
        additional_http_error_status_codes: Iterable[int] = (),
        ignore_http_error_status_codes: Iterable[int] = (),
        **kwargs: Unpack[BasicCrawlerOptions[ParsedHttpCrawlingContext[bytes]]],
    ) -> None:
        """I didn't find another way how to make default constructor specifying one of type on generics."""
        super().__init__(
            parser=NoParser(),
            additional_http_error_status_codes=additional_http_error_status_codes,
            ignore_http_error_status_codes=ignore_http_error_status_codes,
            **kwargs,
        )
