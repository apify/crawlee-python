from __future__ import annotations

import asyncio
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from cssselect import SelectorError
from lxml.html import FormElement
from parsel import Selector

from crawlee._utils.docs import docs_group
from crawlee._utils.html import forms_to_requests, get_declared_html_encoding
from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext

from ._utils import html_to_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from typing_extensions import Self, Unpack

    from crawlee._request import Request
    from crawlee._utils.html import FormRequestOptions


@dataclass(frozen=True)
@docs_group('Crawling contexts')
class ParselCrawlingContext(ParsedHttpCrawlingContext[Selector]):
    """The crawling context used by the `ParselCrawler`.

    It provides access to key objects as well as utility functions for handling crawling tasks.
    """

    @property
    def selector(self) -> Selector:
        """Convenience alias."""
        return self.parsed_content

    @classmethod
    def from_parsed_http_crawling_context(cls, context: ParsedHttpCrawlingContext[Selector]) -> Self:
        """Create a new context from an existing `ParsedHttpCrawlingContext[Selector]`."""
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
            try:
                matches = self.selector.css(selector)
            except (SelectorError, ValueError):  # Parsel raises the latter for invalid XPath, like an unknown prefix.
                return []
            # The selector can also match other elements, text or attributes.
            forms = [match.root for match in matches if isinstance(match.root, FormElement)]

            # `ParselParser` reads a page declaring no encoding as UTF-8.
            page_encoding = get_declared_html_encoding(body, content_type) or 'utf-8'

            return forms_to_requests(
                forms,
                self.request.loaded_url or self.request.url,
                page_encoding,
                fields=fields,
                click=click,
                all_forms=all_forms,
                **kwargs,
            )

        # A large page would block the event loop.
        return await asyncio.to_thread(build_requests)
