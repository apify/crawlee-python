from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

import lxml.html
from typing_extensions import override

from crawlee._utils.docs import docs_group

from ._base_distiller import _WHITESPACE_RE, BasePydanticAiHtmlDistiller
from ._prompts import _CLEAN_HTML_PROMPT_NOTES, _TRUNCATION_MARKER
from ._utils import get_basic_http_cleaner

if TYPE_CHECKING:
    from lxml.html import HtmlElement
    from lxml_html_clean import Cleaner


# Attributes that carry selector targets or semantic meaning for an LLM.
_SEMANTIC_ATTRS = frozenset(
    {
        'class',
        'id',
        'itemprop',
        'itemtype',
        'href',
        'src',
        'alt',
        'title',
        'name',
        'property',
        'content',
        'datetime',
        'role',
        'type',
        'value',
        'placeholder',
        'aria-label',
        'lang',
        'for',
    }
)

logger = getLogger(__name__)


@docs_group('Other')
class PydanticAiCleanHtmlDistiller(BasePydanticAiHtmlDistiller):
    """Distiller that produces cleaned, structure-preserving HTML for direct LLM extraction.

    The full page text survives, so the data to extract lives inside the produced document. Tags, nesting, and
    semantic attributes (`class`, `itemprop`, `datetime`) are kept so the model can tell fields apart.

    JSON scripts are kept in full by default. For sites where a JSON-LD or framework blob is itself the data, this
    is the cheapest path. Such blobs can reach hundreds of kilobytes, so set `max_json_len` for them.

    This is the default distiller for `PydanticAiDirectExtractor`. See `PydanticAiSkeletonDistiller` for the
    selector-generation variant.

    ### Usage

    ```python
    from crawlee.crawlers import PydanticAiCleanHtmlDistiller

    distiller = PydanticAiCleanHtmlDistiller(max_json_len=5_000)
    distilled_html = distiller.distill('<html>...</html>')
    ```
    """

    def __init__(
        self,
        *,
        cleaner: Cleaner | None = None,
        max_classes: int = 5,
        max_attr_len: int = 300,
        max_json_len: int | None = None,
        keep_head: bool = True,
        max_size: int | None = 400_000,
        pretty: bool = False,
        prompt_notes: str | None = _CLEAN_HTML_PROMPT_NOTES,
    ) -> None:
        """Initialize a new instance.

        Args:
            cleaner: A custom `lxml_html_clean.Cleaner`.
            max_classes: How many class tokens to keep per element.
            max_attr_len: Cap on attribute value length, in characters.
            max_json_len: Cap on JSON payload length, or `None` to keep in full.
            keep_head: Whether to keep a reduced `<head>` containing `<title>`, semantic `<meta>` and JSON scripts.
            max_size: Hard cap on the distilled document, in characters. When breached, the tail is dropped and
                replaced with the truncation marker.
            pretty: Whether to pretty-print the serialized HTML.
            prompt_notes: Override for the default prompt notes. Pass `None` to send no notes to the LLM.
        """
        super().__init__(prompt_notes=prompt_notes)
        self._cleaner = cleaner or get_basic_http_cleaner()
        self._max_classes = max_classes
        self._max_attr_len = max_attr_len
        self._max_json_len = max_json_len
        self._keep_head = keep_head
        self._max_size = max_size
        self._pretty = pretty

    @override
    def distill(self, html: str) -> str:
        """Convert raw HTML to the cleaned, structure-preserving representation.

        Args:
            html: The raw HTML markup.
        """
        if not html or not html.strip():
            return ''

        tree = self._parse_and_clean(html)
        self._reduce(tree)
        distilled_html = self._serialize(tree)

        result = self._enforce_max_size(distilled_html, html)

        logger.debug(f'{type(self).__name__} distilled {len(html)} chars to {len(result)} chars.')

        return result

    def _parse_and_clean(self, html: str) -> HtmlElement:
        """Parse raw HTML and run the cleaning stage in place.

        Args:
            html: The raw HTML markup.
        """
        tree = lxml.html.fromstring(html)

        self._protect_json_scripts(tree)

        self._cleaner(tree)

        self._unprotect_json_scripts(tree)

        return tree

    def _reduce(self, tree: HtmlElement) -> None:
        """Apply reduction passes to the cleaned tree in place.

        Args:
            tree: The cleaned lxml tree.
        """
        self._reduce_head(tree)
        self._filter_attributes(tree)
        self._truncate_json_scripts(tree)
        self._normalize_text(tree)

    def _reduce_head(self, tree: HtmlElement) -> None:
        """Reduce `<head>` to its useful children, or drop it entirely.

        Args:
            tree: The lxml tree.
        """
        head = tree.find('head')
        if head is None:
            return

        if not self._keep_head:
            head.getparent().remove(head)
            return

        for child in head:
            keep_child = (
                child.tag == 'title'
                or self._is_json_script(child)
                # meta with `name` or `property` carries structured data, everything else is noise.
                or (child.tag == 'meta' and (child.get('name') or child.get('property')))
            )
            if not keep_child:
                head.remove(child)

    def _filter_attributes(self, tree: HtmlElement) -> None:
        """Drop attributes outside the semantic allowlist, truncate long values.

        Args:
            tree: The lxml tree.
        """
        for elem in tree.iter():
            if not isinstance(elem.tag, str):
                continue

            for name in list(elem.attrib):
                if name in _SEMANTIC_ATTRS or name.startswith(('data-', 'aria-')):
                    value = elem.attrib[name]

                    # Inline `data:` URIs (base64 images and the like) are pure noise for an LLM.
                    if value.lstrip().lower().startswith('data:'):
                        del elem.attrib[name]
                        continue

                    if name == 'class':
                        kept_classes = ' '.join(value.split()[: self._max_classes])
                        if kept_classes:
                            elem.attrib[name] = kept_classes
                        else:
                            del elem.attrib[name]
                    elif len(value) > self._max_attr_len:
                        elem.attrib[name] = f'{value[: self._max_attr_len]}{_TRUNCATION_MARKER}'
                else:
                    del elem.attrib[name]

    def _truncate_json_scripts(self, tree: HtmlElement) -> None:
        """Cap JSON script payloads to `max_json_len` characters, when set.

        Args:
            tree: The lxml tree.
        """
        if self._max_json_len is None:
            return
        for elem in tree.iter('script'):
            if self._is_json_script(elem) and elem.text and len(elem.text) > self._max_json_len:
                # Truncated JSON is invalid JSON, but the LLM only needs to see the key structure to anchor
                # selectors or read top-level fields.
                elem.text = elem.text[: self._max_json_len] + _TRUNCATION_MARKER

    def _normalize_text(self, tree: HtmlElement) -> None:
        """Collapse whitespace runs in text and tail content.

        Args:
            tree: The lxml tree.
        """
        for elem in tree.iter():
            if not isinstance(elem.tag, str):
                continue

            if elem.text and not self._is_json_script(elem):
                elem.text = _WHITESPACE_RE.sub(' ', elem.text)
            if elem.tail:
                elem.tail = _WHITESPACE_RE.sub(' ', elem.tail)

    def _serialize(self, tree: HtmlElement) -> str:
        """Serialize the lxml tree to an HTML string.

        Args:
            tree: The lxml tree.
        """
        return lxml.html.tostring(tree, encoding='unicode', pretty_print=self._pretty)

    def _enforce_max_size(
        self,
        distilled_html: str,
        html: str,  # noqa: ARG002 exposed for subclasses that prefer re-distillation
    ) -> str:
        """Apply the size budget by cutting the tail and appending the marker.

        Args:
            distilled_html: The distilled output to size-check.
            html: The original markup.
        """
        if self._max_size is not None and len(distilled_html) > self._max_size:
            # No safe way to cut HTML mid-stream without breaking the structure.
            logger.warning(
                f'{type(self).__name__} output exceeds max_size ({len(distilled_html)} > {self._max_size}). '
                'The tail of the page is cut off and invisible to the LLM. '
                'Raise `max_size` or set `max_json_len`.'
            )
            return distilled_html[: self._max_size] + _TRUNCATION_MARKER

        return distilled_html
