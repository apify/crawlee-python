from __future__ import annotations

import asyncio
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from crawlee._utils.docs import docs_group
from crawlee._utils.forms import forms_to_requests, parse_html, response_charset
from crawlee.crawlers import ParsedHttpCrawlingContext

from ._utils import html_to_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from typing_extensions import Self, Unpack

    from crawlee._request import Request
    from crawlee._types import HttpHeaders
    from crawlee._utils.forms import FormRequestOptions


@dataclass(frozen=True)
@docs_group('Crawling contexts')
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
        """Initialize a new instance from an existing `ParsedHttpCrawlingContext`."""
        return cls(**{field.name: getattr(context, field.name) for field in fields(context)})

    def html_to_text(self) -> str:
        """Convert the parsed HTML content to newline-separated plain text without tags."""
        return html_to_text(self.parsed_content)

    async def form_requests(
        self,
        selector: str = 'form',
        *,
        form_data: Mapping[str, str | Sequence[str] | None] | None = None,
        click_data: Mapping[str, str] | None = None,
        dont_click: bool = False,
        headers: HttpHeaders | dict[str, str] | None = None,
        **kwargs: Unpack[FormRequestOptions],
    ) -> list[Request]:
        """Create a `Request` submitting each form on the page, the way a browser does.

        Forms with no submit button matching `click_data`, dialog forms and forms not submitting over HTTP(S) are
        skipped. The page is parsed again with lxml from the raw response body, so the forms don't depend on the
        parser `BeautifulSoup` was created with.

        Args:
            selector: CSS selector for the forms to submit.
            form_data: Field values overriding those in the form. A `None` value drops the field.
            click_data: Attributes identifying the submit button to click. Defaults to the first one.
            dont_click: Submit the form without clicking any button.
            headers: The HTTP headers of the request. The `Content-Type` of the form is added to them.
            **kwargs: Additional options passed to `Request.from_url`.
        """
        body = await self.http_response.read()
        content_type = self.http_response.headers.get('content-type')
        header_charset = response_charset(content_type)

        def build_requests() -> list[Request]:
            # Fields are read from an lxml tree of the page, whose forms are matched to the soup ones by position.
            # That only holds for a soup built by lxml, so other parsers need a re-parse.
            if self.soup.builder.NAME == 'lxml':
                soup = self.soup
            else:
                soup = BeautifulSoup(body, 'lxml', from_encoding=header_charset)

            selected = [tag for tag in soup.select(selector) if tag.name == 'form']
            if not selected:
                return []

            # The header charset wins, as in browsers. Otherwise decode like the soup, which also sniffs pages that
            # don't declare an encoding. The outdated `types-beautifulsoup4` stubs resolve `original_encoding` as a tag.
            encoding = header_charset or soup.original_encoding
            lxml_forms = list(parse_html(body, encoding).iter('form'))  # ty: ignore[invalid-argument-type]

            position_by_form = {id(form): position for position, form in enumerate(soup.find_all('form'))}
            forms = [lxml_forms[position_by_form[id(form)]] for form in selected]

            return forms_to_requests(
                forms,
                self.request.loaded_url or self.request.url,
                content_type,
                form_data=form_data,
                click_data=click_data,
                dont_click=dont_click,
                headers=headers,
                **kwargs,
            )

        return await asyncio.to_thread(build_requests)
