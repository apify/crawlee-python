from __future__ import annotations

from crawlee._utils.globs import Glob


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


def test_case_insensitive() -> None:
    glob = Glob('https://Someplace.com/**/cats')
    assert glob.regexp.search('https://someplace.com/blog/category/cats') is not None
    assert glob.regexp.search('https://Someplace.com/blog/category/cats') is not None


def test_search_matches_whole_string_only() -> None:
    # URL filters match with `search` (aligned with `regexp.test` in crawlee-js), so the pattern must
    # stay anchored to the whole string - it must not match a URL merely containing the pattern.
    glob = Glob('https://example.com/*')
    assert glob.regexp.search('https://evil.com/redirect?to=https://example.com/x') is None
