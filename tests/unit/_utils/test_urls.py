from __future__ import annotations

from pydantic import HttpUrl

from crawlee._utils.urls import is_url_absolute, make_url_absolute


def test_is_url_absolute() -> None:
    assert is_url_absolute('http://example.com/path') is True
    assert is_url_absolute('https://example.com/path') is True
    assert is_url_absolute('ftp://example.com/path') is True
    assert is_url_absolute('/path/to/resource') is False
    assert is_url_absolute('relative/path/to/resource') is False
    assert is_url_absolute('example.com/path') is False
    assert is_url_absolute(HttpUrl('http://example.com/path')) is True


def test_make_url_absolute() -> None:
    base_url: str | HttpUrl = 'http://example.com'
    relative_url = '/path/to/resource'
    absolute_url = make_url_absolute(base_url, relative_url)
    assert str(absolute_url) == 'http://example.com/path/to/resource'

    base_url = HttpUrl('http://example.com')
    relative_url = 'path/to/resource'
    absolute_url = make_url_absolute(base_url, relative_url)
    assert str(absolute_url) == 'http://example.com/path/to/resource'

    base_url = 'http://example.com/base/'
    relative_url = '../path/to/resource'
    absolute_url = make_url_absolute(base_url, relative_url)
    assert str(absolute_url) == 'http://example.com/path/to/resource'
