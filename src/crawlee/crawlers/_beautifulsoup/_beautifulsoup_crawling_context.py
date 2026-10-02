from __future__ import annotations

import asyncio
import codecs
import re
from contextlib import suppress
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from lxml.etree import ParserError  # ty: ignore[unresolved-import]
from lxml.html import HTMLParser, document_fromstring
from soupsieve import SelectorSyntaxError

from crawlee._utils.docs import docs_group
from crawlee._utils.html import (
    decode_html_body,
    forms_to_requests,
    get_declared_html_encoding,
    resolve_encoding,
    strip_html_comments,
)
from crawlee.crawlers import ParsedHttpCrawlingContext

from ._utils import html_to_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from typing_extensions import Self, Unpack

    from crawlee._request import Request
    from crawlee._utils.html import FormRequestOptions


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

    async def extract_form_requests(
        self,
        *,
        selector: str = 'form',
        fields: Mapping[str, str | Sequence[str] | None] | None = None,
        click: bool | Mapping[str, str] = True,
        all_forms: bool = False,
        **kwargs: Unpack[FormRequestOptions],
    ) -> list[Request]:
        """Create requests submitting the forms matching `selector` the way a browser does.

        By default, only one form is submitted: the first one a browser can submit among those sharing the most field
        names with `fields`.

        Args:
            selector: CSS selector for the forms. An invalid one matches no form.
            fields: Values replacing the form fields or adding new ones. A `None` value drops the field and a sequence
                submits it once per value. With `all_forms`, only the fields a form has are replaced.
            click: The submit button to click: `True` for the first enabled one, if any, `False` for none, or a
                mapping of attributes the button must have, disabled or not, which skips forms without such a button.
            all_forms: Submit each matching form instead of only one.
            **kwargs: Additional options passed to `Request.from_url`.
        """
        body = await self.http_response.read()
        content_type = self.http_response.headers.get('content-type')

        def build_requests() -> list[Request]:
            declared_encoding = get_declared_html_encoding(body, content_type)
            page_encoding = declared_encoding or _guessed_encoding(body)
            text = decode_html_body(body, page_encoding)
            # Searching the text is much cheaper than searching a tree.
            if not re.search('<form', text, re.IGNORECASE):
                return []

            # Soup forms are matched to the lxml ones by position, which only holds for a soup built by lxml from the
            # same text. `BeautifulSoupParser` decodes a page declaring no encoding in its own way.
            selected = None
            if selector != 'form':
                reuse_soup = declared_encoding and self.soup.builder.NAME == 'lxml'
                soup = self.soup if reuse_soup else BeautifulSoup(text, 'lxml')
                # Soupsieve raises `NotImplementedError` for pseudo-elements and `ValueError` for escapes past Unicode.
                try:
                    selected = {i for i, tag in enumerate(soup.find_all('form')) if tag.css.match(selector)}
                except (SelectorSyntaxError, NotImplementedError, ValueError):
                    return []
                if not selected:
                    return []

            try:
                root = document_fromstring(text.encode(), parser=HTMLParser(encoding='utf-8', huge_tree=True))
            except ParserError:  # A page of only comments has no elements.
                return []
            # Newer libxml2 puts forms after `</html>` into a second root, which isn't searched, so the soup forms there
            # match no position.
            forms = [form for i, form in enumerate(root.iter('form')) if selected is None or i in selected]

            return forms_to_requests(
                forms,
                self.request.loaded_url or self.request.url,
                page_encoding,
                fields=fields,
                click=click,
                all_forms=all_forms,
                **kwargs,
            )

        return await asyncio.to_thread(build_requests)


def _guessed_encoding(body: bytes) -> str:
    """Get the Python codec for the encoding `BeautifulSoup` picks for a page declaring none in the prescan."""
    # Browsers switch to a `<meta>` charset found past the prescan, but not to a commented-out one.
    detector = EncodingDetector(strip_html_comments(body), is_html=True)
    for name in detector.encodings:
        # Browsers ignore a declared label they don't know, like `base64`.
        if name == detector.declared_encoding and not resolve_encoding(name):
            continue
        with suppress(LookupError):
            return resolve_encoding(name) or codecs.lookup(name).name
    return 'utf-8'
