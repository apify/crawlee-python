from __future__ import annotations

import pytest

from crawlee.storage_backends._utils import SortedKeys


@pytest.mark.parametrize(
    ('changes', 'expected_keys'),
    [
        pytest.param([('add', 'b')], ['a', 'b', 'c'], id='add a key'),
        pytest.param([('add', 'a')], ['a', 'c'], id='add a key that is there'),
        pytest.param([('discard', 'a')], ['c'], id='discard a key'),
        pytest.param([('discard', 'x')], ['a', 'c'], id='discard a key that is not there'),
        pytest.param([('discard', 'a'), ('add', 'a')], ['a', 'c'], id='add a discarded key'),
        pytest.param([('add', 'b'), ('discard', 'b')], ['a', 'c'], id='discard an added key'),
    ],
)
def test_page_reflects_changes_since_previous_page(changes: list[tuple[str, str]], expected_keys: list[str]) -> None:
    """A page reflects the keys added and discarded since the previous page."""
    sorted_keys = SortedKeys(['c', 'a'])
    sorted_keys.page(prefix=None, exclusive_start_key=None, limit=10)

    for method, key in changes:
        getattr(sorted_keys, method)(key)

    keys, next_exclusive_start_key = sorted_keys.page(prefix=None, exclusive_start_key=None, limit=10)

    assert keys == expected_keys
    assert next_exclusive_start_key is None


def test_discarding_unknown_keys_keeps_nothing_to_merge() -> None:
    """Keys that are discarded without ever being in the sorted keys aren't kept for the next merge."""
    sorted_keys = SortedKeys(['a'])
    sorted_keys.add('x')

    for key in ['x', 'y', 'z']:
        sorted_keys.discard(key)

    assert sorted_keys._added == set()
    assert sorted_keys._removed == set()
