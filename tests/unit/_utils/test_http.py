from __future__ import annotations

from unittest.mock import Mock

import pytest

from crawlee import HttpHeaders
from crawlee._utils.http import drain_small_body, parse_content_type_charset
from crawlee.http_clients import HttpResponse


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


@pytest.mark.parametrize(
    ('headers', 'expected_reads'),
    [
        pytest.param({'content-length': '1024'}, 1, id='small-body'),
        pytest.param({'content-length': str(1024 * 1024)}, 0, id='large-body'),
        pytest.param({}, 0, id='unknown-length'),
    ],
)
async def test_drain_small_body_reads_only_small_body(headers: dict[str, str], expected_reads: int) -> None:
    """Test that `drain_small_body` reads the body only when it is known to be small."""
    response = Mock(spec=HttpResponse, headers=HttpHeaders(headers))

    await drain_small_body(response)

    assert response.read.await_count == expected_reads


async def test_drain_small_body_ignores_failed_read() -> None:
    """Test that `drain_small_body` doesn't raise when reading the body fails."""
    response = Mock(spec=HttpResponse, headers=HttpHeaders({'content-length': '10'}))
    response.read.side_effect = ConnectionError

    await drain_small_body(response)
