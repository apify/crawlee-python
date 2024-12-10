from __future__ import annotations

import re

from bs4 import BeautifulSoup, NavigableString, Tag, PageElement


SKIP_TAGS = {"script", "style" "canvas", "svg", "noscript", "title"}
BLOCK_TAGS = {"p" , "h1", "h2", "h3", "h4", "h5", "h6", "ol", "ul", "li", "pre", "address", "blockquote","dl", "div","fieldset", "form", "table" ,"tr","select","option"}

def html_to_text(source: str | BeautifulSoup) -> str:
    """Converts markup string or BeautifulSoup object to newline separated plain text without tags."""
    if isinstance(source, str):
        soup = BeautifulSoup(source)
    elif isinstance(source, BeautifulSoup):
        soup = source
    else:
        raise TypeError('Source must be either a string or a BeautifulSoup object.')

    text = ""

    def _page_element_to_text(page_element: PageElement) -> str:
        nonlocal text
        if isinstance(page_element, NavigableString):
            compr: str
            if page_element.parent.name.lower() == 'pre':
                compr = page_element.get_text()
            else:
                # Compares white spaces outside of pre block
                compr = re.sub(r"\s+", " ", page_element.get_text())
            if compr.startswith(" ") and re.match(r"^|\s", page_element.get_text()):
                compr = compr[1:]
            text += compr
            if page_element.parent.name.lower() == 'br':
                text += "\n"
            if page_element.parent.name.lower() == 'td':
                text += "\t"
            if page_element.parent.name.lower() in BLOCK_TAGS:
                text = f"\n{compr}"
            return compr

        if isinstance(page_element, Tag) and page_element.name.lower() in SKIP_TAGS:
            return ""
        x = list(page_element.stripped_strings)
        text_parts = [_page_element_to_text(child) for child in page_element.children]

        return "".join(text_parts)




    return _page_element_to_text(soup)



