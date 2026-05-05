from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from crawlee._utils.urls import (
    convert_to_absolute_url,
    filter_url,
    is_url_absolute,
    validate_http_url,
)

if TYPE_CHECKING:
    from crawlee._types import EnqueueStrategy


def test_is_url_absolute() -> None:
    assert is_url_absolute('http://example.com/path') is True
    assert is_url_absolute('https://example.com/path') is True
    assert is_url_absolute('ftp://example.com/path') is True
    assert is_url_absolute('//example.com/path') is False
    assert is_url_absolute('/path/to/resource') is False
    assert is_url_absolute('relative/path/to/resource') is False
    assert is_url_absolute('example.com/path') is False


def test_convert_to_absolute_url() -> None:
    base_url = 'http://example.com'
    relative_url = '/path/to/resource'
    absolute_url = convert_to_absolute_url(base_url, relative_url)
    assert absolute_url == 'http://example.com/path/to/resource'

    base_url = 'http://example.com'
    relative_url = '//example.com/path/to/resource'
    absolute_url = convert_to_absolute_url(base_url, relative_url)
    assert absolute_url == 'http://example.com/path/to/resource'

    base_url = 'http://example.com/base/'
    relative_url = '../path/to/resource'
    absolute_url = convert_to_absolute_url(base_url, relative_url)
    assert absolute_url == 'http://example.com/path/to/resource'


def test_validate_http_url() -> None:
    assert validate_http_url(None) is None

    valid_url = 'https://example.com'
    assert validate_http_url(valid_url) == valid_url


@pytest.mark.parametrize(
    'invalid_url',
    [
        'htp://invalid-url',
        'gopher://127.0.0.1:6379/_PING',
        'file:///etc/passwd',
        'dict://127.0.0.1:11211/stat',
        'ftp://example.com/secret.txt',
        'javascript:alert(1)',
        'example.com/path',
    ],
)
def test_validate_http_url_rejects_non_http_scheme(invalid_url: str) -> None:
    with pytest.raises(ValidationError):
        validate_http_url(invalid_url)


@pytest.mark.parametrize(
    ('strategy', 'origin', 'target', 'expected'),
    [
        # 'all' lets http(s) through across hosts, but rejects non-http(s) schemes
        ('all', 'https://example.com/', 'https://other.test/', True),
        ('all', 'https://example.com/', 'gopher://internal:6379/_PING', False),
        ('all', 'https://example.com/', 'mailto:foo@bar.com', False),
        ('all', 'https://example.com/', 'javascript:alert(1)', False),
        ('all', 'https://example.com/', 'ftp://example.com/', False),
        # 'same-hostname' is exact host equality
        ('same-hostname', 'https://example.com/a', 'https://example.com/b', True),
        ('same-hostname', 'https://example.com/', 'https://www.example.com/', False),
        ('same-hostname', 'https://example.com/', 'https://other.test/', False),
        ('same-hostname', 'https://example.com/', 'mailto:foo@example.com', False),
        # 'same-domain' allows subdomains under the same registrable domain
        ('same-domain', 'https://example.com/', 'https://www.example.com/', True),
        ('same-domain', 'https://example.com/', 'https://api.example.com/', True),
        ('same-domain', 'https://example.com/', 'https://other.test/', False),
        ('same-domain', 'https://example.com/', 'ftp://www.example.com/', False),
        # 'same-origin' requires scheme + host + port match
        ('same-origin', 'https://example.com/', 'https://example.com/path', True),
        ('same-origin', 'https://example.com/', 'http://example.com/', False),
        ('same-origin', 'https://example.com/', 'https://example.com:8443/', False),
        # missing hostname rejects everything except 'all'
        ('same-hostname', 'https://example.com/', 'not-a-url', False),
        ('same-domain', 'not-a-url', 'https://example.com/', False),
    ],
)
def test_filter_url(strategy: EnqueueStrategy, origin: str, target: str, *, expected: bool) -> None:
    ok, reason = filter_url(target=target, strategy=strategy, origin=origin)
    assert ok is expected
    assert (reason is None) is expected
