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
