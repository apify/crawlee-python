from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from crawlee._utils.html_to_text import html_to_text

_EXPECTED_LINES = ('line 1', 'line2', 'line3')
_EXAMPLE_HTML = f'<a href="http://example.com/">{_EXPECTED_LINES[0]}<i>{_EXPECTED_LINES[1]}</i>\n</a><code>{_EXPECTED_LINES[2]}</code>'


@pytest.mark.parametrize('source', [_EXAMPLE_HTML, BeautifulSoup(_EXAMPLE_HTML)], ids=('String', 'BeautifulSoup'))
def test_html_to_text(source: str | BeautifulSoup) -> None:
    assert html_to_text(source) == '\n'.join(_EXPECTED_LINES)


def test_html_to_text_raises_on_wrong_input_type() -> None:
    with pytest.raises(TypeError):
        html_to_text(1)  # type: ignore[arg-type]  # Intentional wrong type test.
