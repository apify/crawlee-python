from __future__ import annotations

import pytest

from crawlee._utils.lru_cache import LRUCache


@pytest.fixture()
def lru_cache() -> LRUCache[int]:
    cache = LRUCache[int](3)
    cache['a'] = 1
    cache['c'] = 3
    cache['b'] = 2
    return cache


def test_get(lru_cache: LRUCache[int]) -> None:
    # Key error with non-existent key
    with pytest.raises(KeyError):
        _ = lru_cache['non-existent-key']
    # None when using .get instead
    assert lru_cache.get('non-existent-key') is None
    # Should return correct value for existing key
    assert lru_cache['c'] == 3
    # Check if order of keys changed based on LRU rule
    for actual, target in zip(lru_cache, ['a', 'b', 'c']):
        assert actual == target


def test_set(lru_cache: LRUCache[int]) -> None:
    assert len(lru_cache) == 3
    lru_cache['d'] = 4
    # Check if max_length is not exceeded
    assert len(lru_cache) == 3
    # Check if oldest key is removed
    assert 'a' not in lru_cache
    # Check if the newest addition is at the end
    assert list(lru_cache.items())[-1] == ('d', 4)


def test_del(lru_cache: LRUCache[int]) -> None:
    # Key error on non-existent key
    with pytest.raises(KeyError):
        del lru_cache['non-existent-key']
    # No error with existing key
    len_before_del = len(lru_cache)
    del lru_cache['a']
    assert len(lru_cache) == len_before_del - 1
    assert 'a' not in lru_cache


def test_len(lru_cache: LRUCache[int]) -> None:
    assert len(lru_cache) == len(lru_cache._cache)
    lru_cache.clear()
    assert len(lru_cache) == 0


def test_iter(lru_cache: LRUCache[int]) -> None:
    assert list(lru_cache) == ['a', 'c', 'b']
