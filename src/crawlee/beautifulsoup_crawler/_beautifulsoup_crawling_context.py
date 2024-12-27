from dataclasses import dataclass, fields
from typing import Any, Unpack

from bs4 import BeautifulSoup
from typing_extensions import Self

from crawlee._request import BaseRequestData
from crawlee._types import EnqueueLinksKwargs, EnqueueStrategy
from crawlee._utils.docs import docs_group
from crawlee._utils.urls import is_url_absolute, convert_to_absolute_url
from crawlee.abstract_http_crawler._http_crawling_context import ParsedHttpCrawlingContext
from crawlee.beautifulsoup_crawler._utils import html_to_text


@dataclass(frozen=True)
@docs_group('Data structures')
class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup]):
    """The crawling context used by the `BeautifulSoupCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    @property
    def soup(self) -> BeautifulSoup:
        """Convenience alias."""
        return self.parsed_content

    @classmethod
    def from_parsed_http_crawling_context(cls, context: ParsedHttpCrawlingContext[BeautifulSoup]) -> Self:
        """Convenience constructor that creates new context from existing `ParsedHttpCrawlingContext[BeautifulSoup]`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    def html_to_text(self) -> str:
        """Convert the parsed HTML content to newline-separated plain text without tags."""
        return html_to_text(self.parsed_content)

    async def enqueue_links(self,
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
        for link in self._parser.find_links(self.parsed_content, selector=selector):
            url = link
            if not is_url_absolute(url):
                url = convert_to_absolute_url(self.request.url, url)
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
