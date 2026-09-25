from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from parsel import Selector

from crawlee._utils.docs import docs_group
from crawlee._utils.forms import forms_to_requests
from crawlee.crawlers._abstract_http import ParsedHttpCrawlingContext

from ._utils import html_to_text

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from typing_extensions import Self, Unpack

    from crawlee._request import Request
    from crawlee._types import HttpHeaders
    from crawlee._utils.forms import FormRequestOptions


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
        skipped.

        Args:
            selector: CSS selector for the forms to submit.
            form_data: Field values overriding those in the form. A `None` value drops the field.
            click_data: Attributes identifying the submit button to click. Defaults to the first one.
            dont_click: Submit the form without clicking any button.
            headers: The HTTP headers of the request. The `Content-Type` of the form is added to them.
            **kwargs: Additional options passed to `Request.from_url`.
        """
        forms = []
        for match in self.selector.css(selector):
            # The selector can also match text or attributes, whose root is a plain string.
            element = match.root
            if getattr(element, 'tag', None) == 'form':
                forms.append(element)

        return forms_to_requests(
            forms,
            self.request.loaded_url or self.request.url,
            self.http_response.headers.get('content-type'),
            form_data=form_data,
            click_data=click_data,
            dont_click=dont_click,
            headers=headers,
            **kwargs,
        )
