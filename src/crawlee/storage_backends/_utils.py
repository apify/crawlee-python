from __future__ import annotations

import bisect
import itertools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from crawlee.storage_backends.models import KeyValueStoreRecordMetadata


def batch_records_by_size(
    records: Iterable[KeyValueStoreRecordMetadata],
    *,
    max_records: int,
    max_bytes: int,
) -> Iterator[list[KeyValueStoreRecordMetadata]]:
    """Group record metadata into batches bounded by record count and by total record size.

    Each batch is meant to be read in a single call, so the size bound keeps a store with large values from loading
    too many of them into memory at once. A record whose size alone exceeds `max_bytes` is still yielded, but alone in
    its batch. A record with unknown size counts as empty.
    """
    batch: list[KeyValueStoreRecordMetadata] = []
    batch_size = 0

    for record in records:
        record_size = record.size or 0
        if batch and (len(batch) >= max_records or batch_size + record_size > max_bytes):
            yield batch
            batch, batch_size = [], 0

        batch.append(record)
        batch_size += record_size

    if batch:
        yield batch


class SortedKeys:
    """The keys of a key-value store kept in sorted order, so that a page of them is listed without sorting them all.

    Added and removed keys are kept aside and merged into the sorted keys on the next `page` call, so a write stays
    cheap even when no listing continues.
    """

    def __init__(self, keys: Iterable[str]) -> None:
        self._keys = sorted(keys)
        self._added = set[str]()
        self._removed = set[str]()

    def add(self, key: str) -> None:
        """Add the key, unless it's already there."""
        self._added.add(key)

    def discard(self, key: str) -> None:
        """Remove the key, if it's there."""
        self._added.discard(key)
        index = bisect.bisect_left(self._keys, key)
        if index < len(self._keys) and self._keys[index] == key:
            self._removed.add(key)

    def page(self, *, prefix: str | None, exclusive_start_key: str | None, limit: int) -> tuple[list[str], str | None]:
        """Return the keys on one page, and the `exclusive_start_key` of the next page or `None` after the last one."""
        self._merge_changes()

        start = 0
        if prefix is not None:
            start = bisect.bisect_left(self._keys, prefix)
        if exclusive_start_key is not None:
            start = max(start, bisect.bisect_right(self._keys, exclusive_start_key))

        # One key over the limit shows whether more keys follow.
        candidates = self._keys[start : start + limit + 1]
        if prefix is not None:
            # The keys with the prefix follow each other in the sorted order.
            candidates = list(itertools.takewhile(lambda key: key.startswith(prefix), candidates))

        keys = candidates[:limit]
        next_exclusive_start_key = keys[-1] if len(candidates) > limit else None
        return keys, next_exclusive_start_key

    def _merge_changes(self) -> None:
        if not self._added and not self._removed:
            return

        changed = self._added | self._removed
        keys = [key for key in self._keys if key not in changed]
        keys.extend(sorted(self._added))
        # Two sorted runs, which the sort merges in linear time.
        keys.sort()

        self._keys = keys
        self._added.clear()
        self._removed.clear()
