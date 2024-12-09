from __future__ import annotations

from bs4 import BeautifulSoup


def html_to_text(source: str | BeautifulSoup) -> str:
    """Converts markup string or BeautifulSoup object to newline separated plain text without tags."""
    if isinstance(source, str):
        soup = BeautifulSoup(source)
    elif isinstance(source, BeautifulSoup):
        soup = source
    else:
        raise TypeError('Source must be either a string or a BeautifulSoup object.')
    return soup.get_text('\n', strip=True)
