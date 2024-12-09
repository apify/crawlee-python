from __future__ import annotations

import re

from bs4 import BeautifulSoup

SKIP_TAGS = {"script", "style" "canvas", "svg", "noscript"}
BLOCK_TAGS = {"p" , "h1", "h2", "h3", "h4", "h5", "h6", "ol", "ul", "li", "pre", "address", "blockquote","dl", "div","fieldset", "form", "table" ,"tr","select","option"}

def html_to_text(source: str | BeautifulSoup) -> str:
    """Converts markup string or BeautifulSoup object to newline separated plain text without tags."""
    if isinstance(source, str):
        soup = BeautifulSoup(source)
    elif isinstance(source, BeautifulSoup):
        soup = source
    else:
        raise TypeError('Source must be either a string or a BeautifulSoup object.')
    for tag in soup.findAll():
        print(tag)
        if tag.c

    return soup.get_text('\n', strip=True)
