from __future__ import annotations

import pytest
from pydantic import ValidationError

from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute, validate_http_url


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

    invalid_url = 'htp://invalid-url'
    with pytest.raises(ValidationError):
        validate_http_url(invalid_url)
