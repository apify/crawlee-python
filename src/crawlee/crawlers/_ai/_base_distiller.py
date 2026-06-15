from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from lxml.html import HtmlElement


# Placeholder tag used to hide JSON scripts from the cleaning pass. The cleaner removes `<script>` but leaves
# unknown tags intact, so renaming protects position, attributes and content. The tag is restored afterwards.
_JSON_SCRIPT_PROTECT_TAG = 'crawlee-json-script'

# Matches any run of whitespace, used to collapse whitespace inside text nodes.
_WHITESPACE_RE = re.compile(r'\s+')


@docs_group('Other')
class BaseAiHtmlDistiller(ABC):
    """Base class for the built-in HTML distillers.

    A distiller reduces raw HTML to a compact representation that an LLM can read cheaply. Subclasses implement
    `distill`. The base stores the prompt notes and returns them from `get_prompt_notes`. Override
    `get_prompt_notes` when the notes depend on several constructor arguments.

    The public interface is the `AiHtmlDistiller` protocol. The concrete distillers are `AiCleanHtmlDistiller`
    and `AiSkeletonDistiller`.
    """

    def __init__(self, *, prompt_notes: str | None = None) -> None:
        """Initialize a new instance.

        Args:
            prompt_notes: Short description of the final representation. Appended to the LLM task instructions by
                extractors. `None` means no notes are appended.
        """
        self._prompt_notes = prompt_notes

    @abstractmethod
    def distill(self, html: str) -> str:
        """Convert raw HTML to a compact representation suitable for an LLM."""

    def get_prompt_notes(self) -> str | None:
        """Return the configured prompt notes, or `None` when not set."""
        return self._prompt_notes

    def _protect_json_scripts(self, tree: HtmlElement) -> None:
        """Rename JSON-bearing `<script>` tags to protect them from the cleaner."""
        for elem in tree.iter('script'):
            if self._is_json_script(elem):
                elem.tag = _JSON_SCRIPT_PROTECT_TAG

    def _unprotect_json_scripts(self, tree: HtmlElement) -> None:
        """Restore the original tag name to JSON-bearing scripts after cleaning."""
        for elem in tree.iter(_JSON_SCRIPT_PROTECT_TAG):
            elem.tag = 'script'

    def _is_json_script(self, element: HtmlElement) -> bool:
        """Check if the element is a `<script>` carrying a JSON payload."""
        if element.tag not in ('script', _JSON_SCRIPT_PROTECT_TAG):
            return False
        type_attr = (element.get('type') or '').lower()
        return type_attr == 'application/json' or type_attr.endswith('+json')
