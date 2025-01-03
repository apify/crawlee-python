from __future__ import annotations

import re

from parsel import Selector

from crawlee._utils.html_to_text import (
    _ANY_CONSECUTIVE_WHITE_SPACES,
    _EMPTY_OR_ENDS_WITH_ANY_WHITE_SPACE,
    _EMPTY_OR_ENDS_WITH_NEW_LINE,
    BLOCK_TAGS,
    SKIP_TAGS,
)


def html_to_text(source: str | Selector) -> str:
    """Converts markup string or `Selector` to newline-separated plain text without tags using Parsel.

    Args:
        source: Input markup string or `Selector` object.

    Returns:
        Newline separated plain text without tags.
    """
    if isinstance(source, str):
        selector = Selector(text=source)
    elif isinstance(source, Selector):
        selector = source
    else:
        raise TypeError('Source must be either a string or a `Selector` object.')

    text = ''

    def _extract_text(elements: list[Selector], *, compress: bool = True) -> None:
        """Custom html parsing that performs as implementation from Javascript version of Crawlee."""
        nonlocal text
        for element in elements:
            tag = element.root.tag if hasattr(element.root, 'tag') else None

            if tag is None:
                # Compress white spaces outside of pre block
                compr = re.sub(_ANY_CONSECUTIVE_WHITE_SPACES, ' ', element.root) if compress else element.root
                # If text is empty or ends with a whitespace, don't add the leading whitespace or new line
                if (compr.startswith((' ', '\n'))) and re.search(_EMPTY_OR_ENDS_WITH_ANY_WHITE_SPACE, text):
                    compr = compr[1:]
                text += compr

            if tag in SKIP_TAGS or not isinstance(tag, str):
                continue

            if tag == 'br':
                text += '\n'
            elif tag == 'td':
                _extract_text(element.xpath('./node()'))
                text += '\t'
            else:
                is_block_tag = tag in BLOCK_TAGS if tag else False

                if is_block_tag and not re.search(_EMPTY_OR_ENDS_WITH_NEW_LINE, text):
                    text += '\n'

                _extract_text(element.xpath('./node()'), compress=tag != 'pre')

                if is_block_tag and not text.endswith('\n'):
                    text += '\n'

    # Start processing the root elements
    _extract_text(selector.xpath('/*'))

    return text.strip()
