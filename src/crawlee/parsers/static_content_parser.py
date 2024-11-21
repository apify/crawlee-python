import asyncio
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Generic, TypeVar, Iterable, Any, Unpack, AsyncGenerator, override

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from crawlee._request import BaseRequestData
from crawlee._types import EnqueueLinksKwargs, EnqueueStrategy
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.urls import is_url_absolute, convert_to_absolute_url
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawlingContext
from crawlee.errors import SessionError
from crawlee.http_clients import HttpCrawlingResult
from crawlee.http_crawler import HttpCrawlingContext

TParseResult = TypeVar("TParseResult")
TCrawlingResult = TypeVar("TParseResult")

@dataclass
class BeautifulSoupResult:
    soup: BeautifulSoup

class StaticContentParser(Generic[TCrawlingResult, TParseResult], ABC):

    @abstractmethod
    def parse(self, content: TCrawlingResult)->TParseResult:...

    @abstractmethod
    def raise_if_blocked(self, result: TParseResult) -> None: ...

    @abstractmethod
    def find_links(self, result: TParseResult, selector: str) -> Iterable[str]: ...


class BeautifulSoupContentParser(StaticContentParser[HttpCrawlingResult, BeautifulSoupResult]):
    def __init__(self, parser: str = "lxml"):
        self.parser = parser

    @override
    def parse(self, input_content: HttpCrawlingResult)->BeautifulSoupResult:
        return BeautifulSoup(input_content.read(), self._parser)

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
        return matched_selectors  # ??? Really needed ???

    @override
    def find_links(self, result: BeautifulSoupResult, selector: str) -> Iterable[str]:
        urls: str = []
        for link in result.soup.select(selector):
            if (url := link.attrs.get('href')) is not None:
                urls.append(url.strip())
        return urls


class BeautifulSoupStaticContentParser(StaticContentParser[BeautifulSoupResult]):
    async def _parse_http_response(
        self,
        context: HttpCrawlingContext,
    ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
        """Parse the HTTP response using the `BeautifulSoup` library and implements the `enqueue_links` function.

        Args:
            context: The current crawling context.

        Yields:
            The enhanced crawling context with the `BeautifulSoup` selector and the `enqueue_links` function.
        """
        soup = await asyncio.to_thread(lambda: BeautifulSoup(context.http_response.read(), self._parser))

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

            link: Tag
            for link in soup.select(selector):
                link_user_data = user_data

                if label is not None:
                    link_user_data.setdefault('label', label)

                if (url := link.attrs.get('href')) is not None:
                    url = url.strip()

                    if not is_url_absolute(url):
                        url = convert_to_absolute_url(context.request.url, url)

                    try:
                        request = BaseRequestData.from_url(url, user_data=link_user_data)
                    except ValidationError as exc:
                        context.log.debug(
                            f'Skipping URL "{url}" due to invalid format: {exc}. '
                            'This may be caused by a malformed URL or unsupported URL scheme. '
                            'Please ensure the URL is correct and retry.'
                        )
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
