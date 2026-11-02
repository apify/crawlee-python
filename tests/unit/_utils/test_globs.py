from __future__ import annotations

import re

import pytest

from crawlee._utils.globs import Glob, matches_url_pattern


def test_asterisk() -> None:
    glob = Glob('foo/*')
    assert glob.regexp.match('bar/') is None
    assert glob.regexp.match('foo/bar') is not None
    assert glob.regexp.match('foo/bar/baz') is None


def test_double_asteritsk() -> None:
    glob = Glob('foo/**')
    assert glob.regexp.match('bar/') is None
    assert glob.regexp.match('foo/bar') is not None
    assert glob.regexp.match('foo/bar/baz') is not None


@pytest.mark.parametrize(
    'url',
    [
        pytest.param('https://example.com/products/abc', id='same casing'),
        pytest.param('https://example.com/Products/ABC', id='uppercase path'),
        pytest.param('HTTPS://EXAMPLE.COM/PRODUCTS/ABC', id='uppercase url'),
    ],
)
def test_glob_is_case_insensitive(url: str) -> None:
    """A glob matches a URL regardless of its casing."""
    assert matches_url_pattern(Glob('https://example.com/products/*'), url)


@pytest.mark.parametrize(
    ('pattern', 'expected'),
    [
        pytest.param(re.compile(r'/products/'), True, id='regex in the middle'),
        pytest.param(re.compile(r'\d+$'), True, id='regex at the end'),
        pytest.param(re.compile(r'^/products/'), False, id='regex anchored at the start'),
        pytest.param(re.compile(r'/Products/'), False, id='regex is case-sensitive'),
        pytest.param(Glob('/products/*'), False, id='glob must match the whole url'),
        pytest.param(Glob('https://example.com/products'), False, id='glob prefix of the url'),
        pytest.param(Glob('https://example.com/products/*'), True, id='glob matching the whole url'),
    ],
)
def test_matches_url_pattern(pattern: re.Pattern[str] | Glob, *, expected: bool) -> None:
    """A regex may match anywhere in the URL, while a glob must match the whole URL."""
    assert matches_url_pattern(pattern, 'https://example.com/products/123') is expected
