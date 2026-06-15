from __future__ import annotations

from itertools import groupby
from logging import getLogger
from typing import TYPE_CHECKING

from lxml import etree  # ty: ignore[unresolved-import]
from typing_extensions import override

from crawlee._utils.docs import docs_group

from ._base_distiller import _WHITESPACE_RE
from ._clean_html_distiller import AiCleanHtmlDistiller
from ._prompts import _SKELETON_PROMPT_NOTES, _TRUNCATION_MARKER

if TYPE_CHECKING:
    from lxml.html import HtmlElement
    from lxml_html_clean import Cleaner


# Attributes that give an element an identity rather than mark it as one instance of a repeating pattern.
_IDENTITY_ATTRS = ('name', 'property', 'itemprop', 'itemtype', 'role', 'type')

# Tags that are never collapsed even when their signature repeats. `<br>` and `<hr>` are layout markers.
_NEVER_COLLAPSE_TAGS = frozenset({'br', 'hr'})

logger = getLogger(__name__)


@docs_group('Other')
class AiSkeletonDistiller(AiCleanHtmlDistiller):
    """Distiller that produces a DOM skeleton used to ask an LLM for CSS selectors.

    The skeleton is built from the page by removing nodes, attributes, and class tokens, or by truncating text. It
    never renames or re-parents elements. So any selector the LLM builds from the skeleton also matches the
    original page.

    This is the default distiller for `AiSelectorExtractor`. See `AiCleanHtmlDistiller` for the direct-extraction
    variant that keeps the full page text.

    On top of the base cleaning:

    - text nodes are truncated to `max_text_len`, so the model sees samples
      rather than full content.
    - JSON payloads are capped at `max_json_len`, so only their key structure
      reaches the model.
    - runs of repeated siblings are collapsed to the first `keep_siblings`
      items plus a comment marker. Siblings with a distinct identity attribute
      (`name`, `property`, `itemprop`, ...) are kept, since a run of `<meta>`
      tags is not a repeating template.
    - if the result still exceeds `max_size`, it is re-distilled with tighter
      settings. Cutting the output is the last resort.

    ### Usage

    ```python
    from crawlee.crawlers import AiSkeletonDistiller

    distiller = AiSkeletonDistiller(max_text_len=80)
    skeleton = distiller.distill('<html>...</html>')
    ```
    """

    def __init__(
        self,
        *,
        cleaner: Cleaner | None = None,
        max_text_len: int = 50,
        max_json_len: int | None = 1_000,
        keep_siblings: int = 3,
        max_classes: int = 5,
        max_attr_len: int = 100,
        keep_head: bool = True,
        max_size: int | None = 60_000,
        pretty: bool = False,
        prompt_notes: str | None = _SKELETON_PROMPT_NOTES,
    ) -> None:
        """Initialize a new instance.

        Args:
            cleaner: A custom `lxml_html_clean.Cleaner`.
            max_text_len: Cap on a text node, in characters.
            max_json_len: Cap on JSON payload length, or `None` to keep in full.
            keep_siblings: How many leading siblings to keep when a repeated run is collapsed.
            max_classes: How many class tokens to keep per element.
            max_attr_len: Cap on attribute value length, in characters.
            keep_head: Whether to keep a reduced `<head>`.
            max_size: Hard cap on the skeleton, in characters. A tightening re-distillation runs first. If the result
                is still too big, the tail is dropped and replaced with the truncation marker.
            pretty: Whether to pretty-print the serialized HTML.
            prompt_notes: Override for the default prompt notes. Pass `None` to send no notes to the LLM.
        """
        super().__init__(
            cleaner=cleaner,
            max_classes=max_classes,
            max_attr_len=max_attr_len,
            max_json_len=max_json_len,
            keep_head=keep_head,
            max_size=max_size,
            pretty=pretty,
            prompt_notes=prompt_notes,
        )
        self._max_text_len = max_text_len
        self._keep_siblings = keep_siblings

    @override
    def _reduce(self, tree: HtmlElement) -> None:
        """Apply base reduction, then collapse repeated sibling runs.

        Args:
            tree: The cleaned lxml tree.
        """
        super()._reduce(tree)
        self._collapse_repeated_siblings(tree)

    def _truncate_text(self, text: str | None) -> str | None:
        """Collapse whitespace, then cap the text length.

        Args:
            text: The text to normalize, or `None`.
        """
        if not text:
            return text

        truncated_text = _WHITESPACE_RE.sub(' ', text)
        if len(truncated_text) > self._max_text_len:
            return truncated_text[: self._max_text_len].rstrip() + _TRUNCATION_MARKER

        return truncated_text

    @override
    def _normalize_text(self, tree: HtmlElement) -> None:
        """Collapse whitespace and truncate text to short samples.

        Args:
            tree: The lxml tree.
        """
        for elem in tree.iter():
            if not isinstance(elem.tag, str):
                continue

            if not self._is_json_script(elem):
                elem.text = self._truncate_text(elem.text)

            elem.tail = self._truncate_text(elem.tail)

    def _collapse_repeated_siblings(self, tree: HtmlElement) -> None:
        """Collapse runs of equivalent siblings to the first few plus a marker.

        Args:
            tree: The lxml tree.
        """

        def signature(el: HtmlElement) -> tuple:
            return (
                el.tag,
                tuple(sorted((el.get('class') or '').split())),
                tuple(el.get(attr) for attr in _IDENTITY_ATTRS),
            )

        for parent in list(tree.iter()):
            if not isinstance(parent.tag, str):
                continue

            children = [child for child in parent if isinstance(child.tag, str)]
            for sig, group_iter in groupby(children, key=signature):
                group = list(group_iter)

                if len(group) <= self._keep_siblings or sig[0] == 'script' or sig[0] in _NEVER_COLLAPSE_TAGS:
                    continue

                saved, dropped = group[: self._keep_siblings], group[self._keep_siblings :]

                for elem in dropped:
                    parent.remove(elem)

                classes = '.'.join(sig[1])
                label = f'{sig[0]}.{classes}' if classes else sig[0]

                saved[-1].addnext(etree.Comment(f' ...{len(dropped)} more <{label}> siblings omitted '))

    @override
    def _enforce_max_size(self, distilled_html: str, html: str) -> str:
        """Tighten the skeleton if it exceeds `max_size`, and cut as a last resort.

        Args:
            distilled_html: The skeleton output to size-check.
            html: The original markup, re-distilled with tighter settings when the budget is breached.
        """
        if self._max_size is None or len(distilled_html) <= self._max_size:
            return distilled_html

        # The skeleton is too big, re-distill with tighter settings.
        tighter = AiSkeletonDistiller(
            cleaner=self._cleaner,
            max_text_len=max(self._max_text_len // 2, 15),
            max_json_len=self._max_json_len,
            keep_siblings=1,
            max_classes=self._max_classes,
            max_attr_len=self._max_attr_len,
            keep_head=self._keep_head,
            max_size=None,  # prevent infinite recursion
            pretty=self._pretty,
        )
        tighter_distilled_html = tighter.distill(html)

        # Still too big, cut mid-stream and warn.
        if len(tighter_distilled_html) > self._max_size:
            logger.warning(
                f'Skeleton exceeds max_size even after tightening ({len(tighter_distilled_html)} > {self._max_size}). '
                'The tail of the page is cut off and invisible to the LLM. '
                'Raise `max_size`, `scope` the extraction, or set `max_json_len`.'
            )
            return tighter_distilled_html[: self._max_size] + _TRUNCATION_MARKER

        return tighter_distilled_html
