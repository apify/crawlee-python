from __future__ import annotations

import re
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup, NavigableString, PageElement, Tag

if TYPE_CHECKING:
    from collections.abc import Iterable

SKIP_TAGS = {'script', 'style', 'canvas', 'svg', 'noscript', 'title'}
BLOCK_TAGS = {
    'p',
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'ol',
    'ul',
    'li',
    'pre',
    'address',
    'blockquote',
    'dl',
    'div',
    'fieldset',
    'form',
    'table',
    'tr',
    'select',
    'option',
}


def html_to_text(source: str | BeautifulSoup) -> str:
    """Converts markup string or BeautifulSoup object to newline separated plain text without tags."""
    if isinstance(source, str):
        soup = BeautifulSoup(source)
    elif isinstance(source, BeautifulSoup):
        soup = source
    else:
        raise TypeError('Source must be either a string or a BeautifulSoup object.')

    text = ''

    def _page_element_to_text(page_elements: Iterable[PageElement]) -> None:
        """Custom html parsing that performs as implementation from Javascript version of Crawlee."""
        nonlocal text
        for page_element in page_elements:
            if isinstance(page_element, (Tag, NavigableString)):
                if isinstance(page_element, NavigableString):
                    compr: str
                    if isinstance(page_element.parent, Tag) and page_element.parent.name.lower() == 'pre':
                        compr = page_element.get_text()
                    else:
                        # Compress white spaces outside of pre block
                        compr = re.sub(r'\s+', ' ', page_element.get_text())
                    # If text is empty or ends with a whitespace, don't add the leading whitespace or new line
                    if (compr.startswith((' ', '\n'))) and re.search(r'(^|\s)$', text):
                        compr = compr[1:]
                    text += compr
                elif page_element.name.lower() in SKIP_TAGS or isinstance(page_element, int):
                    # Skip comments and special elements
                    pass
                elif page_element.name.lower() == 'br':
                    text += '\n'
                elif page_element.name.lower() == 'td':
                    _page_element_to_text(page_element.children)
                    text += '\t'
                else:
                    # Block elements must be surrounded by newlines(unless beginning of text)
                    is_block_tag = page_element.name.lower() in BLOCK_TAGS
                    if is_block_tag and not re.search(r'(^|\n)$', text):
                        text += '\n'
                    _page_element_to_text(page_element.children)
                    if is_block_tag and not text.endswith('\n'):
                        text += '\n'

    _page_element_to_text(soup.children)
    return text.strip()
