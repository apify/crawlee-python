from __future__ import annotations

import pytest

from crawlee._utils.http import parse_content_type_charset


@pytest.mark.parametrize(
    ('content_type', 'expected'),
    [
        pytest.param('text/html; charset=windows-1250', 'windows-1250', id='plain'),
        pytest.param('text/html; charset="windows-1250"', 'windows-1250', id='quoted'),
        pytest.param('text/html;charset = windows-1250 ; foo=bar', 'windows-1250', id='spaces'),
        pytest.param('text/html; CHARSET=Windows-1250', 'Windows-1250', id='uppercase'),
        pytest.param('multipart/form-data; boundary="charset=foo"', None, id='inside-other-parameter'),
        pytest.param('text/html', None, id='missing'),
        pytest.param(None, None, id='no-header'),
    ],
)
def test_parse_content_type_charset(content_type: str | None, expected: str | None) -> None:
    """The charset parameter is read from a `Content-Type` header."""
    assert parse_content_type_charset(content_type) == expected
