from __future__ import annotations

from collections import OrderedDict
from collections.abc import MutableMapping
from typing import Generic, ItemsView, Iterator, TypeVar, ValuesView
from typing import OrderedDict as OrderedDictType

T = TypeVar('T')


class LRUCache(MutableMapping, Generic[T]):
    """Attempt to reimplement LRUCache from `@apify/datastructures` using `OrderedDict`."""

    def __init__(self, max_length: int) -> None:
        """Create a LRUCache with a specific max_length."""
        self._cache: OrderedDictType[str, T] = OrderedDict()
        self._max_length = max_length

    def __getitem__(self, key: str) -> T:
        """Get an item from the cache. Move it to the end if present."""
        val = self._cache[key]
        # No 'key in cache' condition since the previous line would raise KeyError
        self._cache.move_to_end(key)
        return val

    def __setitem__(self, key: str, value: T) -> None:
        """Add an item to the cache. Remove least used item if max_length exceeded."""
        # Sadly TS impl returns bool indicating whether the key was already present or not
        self._cache[key] = value
        if len(self._cache) > self._max_length:
            self._cache.popitem(last=False)

    def __delitem__(self, key: str) -> None:
        """Remove an item from the cache."""
        del self._cache[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over the keys of the cache in order of insertion."""
        return self._cache.__iter__()

    def __len__(self) -> int:
        """Get the number of items in the cache."""
        return len(self._cache)

    def values(self) -> ValuesView[T]:  # Needed so we don't mutate the cache by __getitem__
        """Iterate over the values in the cache in order of insertion."""
        return self._cache.values()

    def items(self) -> ItemsView[str, T]:  # Needed so we don't mutate the cache by __getitem__
        """Iterate over the pairs of (key, value) in the cache in order of insertion."""
        return self._cache.items()
